import asyncio
import importlib
import json
import logging
import os
import signal
import socket
import sys
import traceback
from datetime import datetime, timezone
from multiprocessing import Event, Process, Queue
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel
from uuid_extensions import uuid7

from hyrex.config import EnvVars
from hyrex.dispatcher import DequeuedTask, get_dispatcher
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.worker.logging import LogLevel, init_logging
from hyrex.worker.messages.root_messages import SetExecutorTaskMessage
from hyrex.worker.worker import HyrexWorker


def generate_executor_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"hyrex-executor-{hostname}-{pid}-{timestamp}"


class WorkerExecutor(Process):

    def __init__(
        self,
        root_message_queue: Queue,
        log_level: LogLevel,
        worker_module_path: str,
        executor_id: UUID,
        queue: str,
    ):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.log_level = log_level

        self.root_message_queue = root_message_queue
        self._stop_event = Event()

        self.worker_module_path = worker_module_path
        self.queue = queue
        self.executor_id = executor_id

        self.name = generate_executor_name()
        self.dispatcher = None
        self.task_registry: HyrexRegistry = None

    def load_worker_module_variables(self):
        sys.path.append(str(Path.cwd()))
        module_path, instance_name = self.worker_module_path.split(":")
        # Import the worker module
        worker_module = importlib.import_module(module_path)
        worker_instance: HyrexWorker = getattr(worker_module, instance_name)

        self.task_registry = worker_instance.task_registry
        self.error_callback = worker_instance.error_callback

        if not self.queue:
            self.queue = worker_instance.queue

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            signame = signal.Signals(signum).name
            self.logger.info(f"\nReceived {signame}. Starting graceful shutdown...")
            self._stop_event.set()

        # Register the handler for both SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def process_item(self, task_name: str, args: dict):
        task_func = self.task_registry[task_name]
        context = task_func.context_klass(**args)
        result = asyncio.run(task_func.async_call(context))
        return result

    def fetch_task(self) -> list[DequeuedTask]:
        return self.dispatcher.dequeue(executor_id=self.executor_id, queue=self.queue)

    def mark_task_success(self, task_id: UUID):
        self.dispatcher.mark_success(task_id=task_id)

    def mark_task_failed(self, task_id: UUID):
        self.dispatcher.mark_failed(task_id=task_id)

    def attempt_retry(self, task_id: UUID):
        self.dispatcher.attempt_retry(task_id=task_id)

    def reset_or_cancel_task(self, task_id: UUID):
        self.dispatcher.reset_or_cancel_task(task_id=task_id)

    # Notifies root process of current task being processed.
    def update_current_task(self, task_id: UUID):
        self.root_message_queue.put(
            SetExecutorTaskMessage(executor_id=self.executor_id, task_id=task_id),
        )

    def process(self):
        try:
            tasks: list[DequeuedTask] = self.fetch_task()
            if not tasks:
                # No unprocessed items, clear current task and wait a bit before trying again
                self.update_current_task(None)
                self._stop_event.wait(0.5)
                return

            task = tasks[0]
            # Notify root process of new task
            self.update_current_task(task.id)
            # Set parent task env var for any sub-tasks
            os.environ[EnvVars.PARENT_TASK] = str(task.id)
            result = self.process_item(task.name, task.args)
            del os.environ[EnvVars.PARENT_TASK]

            if result is not None:
                if isinstance(result, BaseModel):
                    result = result.model_dump_json()
                elif isinstance(result, dict):
                    result = json.dumps(result)
                else:
                    raise TypeError("Return value must be JSON-serializable.")

                self.dispatcher.save_result(task.id, result)

            self.mark_task_success(task.id)

            self.logger.info(
                f"Executor {self.name}: Completed processing item {task.id}"
            )

        except Exception as e:
            self.logger.error(f"Executor {self.name}: Error processing item {str(e)}")
            self.logger.error(e)
            self.logger.error("Traceback:\n%s", traceback.format_exc())
            if self.error_callback:
                task_name = locals().get("task.name", "Unknown task name")
                self.error_callback(task_name, e)

            if "task" in locals():
                self.mark_task_failed(task.id)
                self.attempt_retry(task.id)

            self._stop_event.wait(0.5)  # Add delay after error

    def run(self):
        os.setpgrp()
        init_logging(self.log_level)
        self.setup_signal_handlers()

        # Retrieve task registry, error callback, and queue.
        self.load_worker_module_variables()

        self.dispatcher = get_dispatcher()
        self.dispatcher.register_executor(
            executor_id=self.executor_id, executor_name=self.name, queue=self.queue
        )
        self.task_registry.set_dispatcher(self.dispatcher)

        self.logger.info(f"Executor process {self.name} started - checking for tasks.")

        while not self._stop_event.is_set():
            self.process()

        self.stop()

    def stop(self):
        self.logger.info(f"Stopping {self.name}...")
        if self.dispatcher:
            self.dispatcher.disconnect_executor(self.executor_id)
            self.dispatcher.stop()
