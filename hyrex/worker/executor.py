import asyncio
from fnmatch import fnmatch
import importlib
import json
import logging
import os
import random
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
from hyrex.worker.utils import is_process_alive, glob_to_postgres_regex, is_glob_pattern


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

        self.dispatcher = None
        self.task_registry: HyrexRegistry = None

        # To check if root process is running
        self.parent_pid = os.getpid()

    def update_queue_list(self):
        self.logger.debug("Updating internal queue list from pattern...")
        self.queue_list = self.dispatcher.get_queues_for_pattern(
            self.postgres_queue_pattern
        )
        self.logger.debug(f"Queues found: {self.queue_list}")
        if self.queue_list:
            random.shuffle(self.queue_list)
        else:
            self._stop_event.wait(0.5)
            return

        # 3. Update concurrency values

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

    def process_item(self, task_name: str, args: dict):
        task_func = self.task_registry[task_name]
        context = task_func.context_klass(**args)
        result = asyncio.run(task_func.async_call(context))
        return result

    def fetch_task(self, queue: str) -> DequeuedTask:
        return self.dispatcher.dequeue(executor_id=self.executor_id, queue=queue)

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

    def process(self, queue: str, wait_between_tasks: bool = True):
        """Returns True if a task is found and attempted, False otherwise"""
        try:
            task: DequeuedTask = self.fetch_task(queue=queue)
            if not task:
                # No unprocessed items, clear current task and wait a bit before trying again
                self.update_current_task(None)
                if wait_between_tasks:
                    self._stop_event.wait(0.5)
                return False

            # Notify root process of new task
            self.update_current_task(task.id)
            # Set parent task env var for any sub-tasks
            os.environ[EnvVars.PARENT_TASK_ID] = str(task.id)
            result = self.process_item(task.name, task.args)
            del os.environ[EnvVars.PARENT_TASK_ID]

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
            return True

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
            return True

    def check_root_process(self):
        # Confirm parent is still alive
        if not is_process_alive(self.parent_pid):
            self.logger.warning("Root process died unexpectedly. Shutting down.")
            self._stop_event.set()

    def run_static_queue_loop(self):
        while not self._stop_event.is_set():
            # Queue pattern is a static string - fetch from it directly.
            self.process(self.queue)
            self.check_root_process()

    def run_round_robin_loop(self):
        while not self._stop_event.is_set():
            self.update_queue_list()

            no_task_count = 0

            while self.queue_list and not self._stop_event.is_set():
                self.check_root_process()
                queue = self.queue_list.pop()
                # Don't wait if queue doesn't have task, move directly to next one.
                if not self.process(queue=queue, wait_between_tasks=False):
                    no_task_count += 1
                else:
                    no_task_count = 0

                # We're not hitting populated queues - pause and refresh queue list.
                if no_task_count >= 3:
                    break

    def run(self):
        init_logging(self.log_level)

        self.name = generate_executor_name()

        # Retrieve task registry, error callback, and queue.
        self.load_worker_module_variables()

        # Convert queue pattern to Postgres regex syntax if needed.
        if is_glob_pattern(self.queue):
            self.postgres_queue_pattern = glob_to_postgres_regex(self.queue)
            self.logger.debug(
                f"Converted queue glob to Postgres regex syntax: {self.queue} -> {self.postgres_queue_pattern}"
            )
        else:
            self.postgres_queue_pattern = None

        self.dispatcher = get_dispatcher(worker=True)
        self.dispatcher.register_executor(
            executor_id=self.executor_id,
            executor_name=self.name,
            queue=self.queue,
        )
        self.task_registry.set_dispatcher(self.dispatcher)

        # Ignore signals, let main process manage shutdown.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.logger.info(f"Executor process {self.name} started - checking for tasks.")

        try:
            if self.postgres_queue_pattern:
                self.run_round_robin_loop()
            else:
                self.run_static_queue_loop()
        finally:
            self.stop()

    def stop(self):
        self.logger.info(f"Stopping {self.name}...")
        if self.dispatcher:
            self.dispatcher.disconnect_executor(self.executor_id)
            self.dispatcher.stop()
