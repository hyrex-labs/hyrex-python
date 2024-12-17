import asyncio
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
from inspect import signature
from multiprocessing import Event, Process, Queue
from pathlib import Path
from typing import Callable
from uuid import UUID

from pydantic import BaseModel

from hyrex.config import EnvVars
from hyrex.dispatcher import DequeuedTask, get_dispatcher
from hyrex.hyrex_context import (HyrexContext, clear_hyrex_context,
                                 set_hyrex_context)
from hyrex.hyrex_queue import HyrexQueue
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.worker.logging import LogLevel, init_logging
from hyrex.worker.messages.root_messages import SetExecutorTaskMessage
from hyrex.worker.utils import (glob_to_postgres_regex, is_glob_pattern,
                                is_process_alive)
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
        self.queue_list: list[HyrexQueue] = []
        self.executor_id = executor_id

        self.dispatcher = None
        self.task_registry: HyrexRegistry = None

        # To check if root process is running
        self.parent_pid = os.getpid()

    def get_concurrency_for_queue(self, queue_name: str):
        return self.task_registry.get_concurrency_limit(queue_name=queue_name)

    def update_queue_list(self):
        self.queue_list = []
        self.logger.debug("Updating internal queue list from pattern...")
        queue_names = self.dispatcher.get_queues_for_pattern(
            self.postgres_queue_pattern
        )
        self.logger.debug(f"Queues found: {queue_names}")
        if queue_names:
            random.shuffle(queue_names)
        else:
            self._stop_event.wait(0.5)
            return

        for queue_name in queue_names:
            self.queue_list.append(
                HyrexQueue(
                    name=queue_name,
                    concurrency_limit=self.get_concurrency_for_queue(
                        queue_name=queue_name
                    ),
                )
            )

    def load_worker_module_variables(self):
        sys.path.append(str(Path.cwd()))
        module_path, instance_name = self.worker_module_path.split(":")
        # Import the worker module
        worker_module = importlib.import_module(module_path)
        worker_instance: HyrexWorker = getattr(worker_module, instance_name)

        self.task_registry = worker_instance.task_registry

        if not self.queue:
            self.queue = worker_instance.queue

    def process_item(self, task: DequeuedTask):
        task_func = self.task_registry.get_task(task.task_name)
        context = task_func.context_klass(**task.args)
        result = asyncio.run(task_func.async_call(context))
        return result

    def fetch_task(self, queue: str, concurrency_limit: int = 0) -> DequeuedTask:
        return self.dispatcher.dequeue(
            executor_id=self.executor_id,
            queue=queue,
            concurrency_limit=concurrency_limit,
        )

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

    def process(self, queue: HyrexQueue):
        """Returns True if a task is found and attempted, False otherwise"""
        try:

            task: DequeuedTask = self.fetch_task(
                queue=queue.name, concurrency_limit=queue.concurrency_limit
            )
            if not task:
                return False

            set_hyrex_context(
                HyrexContext(
                    task_id=task.id,
                    root_id=task.root_id,
                    parent_id=task.parent_id,
                    task_name=task.task_name,
                    queue=task.queue,
                    priority=task.priority,
                    scheduled_start=task.scheduled_start,
                    queued=task.queued,
                    started=task.started,
                    executor_id=self.executor_id,
                )
            )

            # Notify root process of new task
            self.update_current_task(task.id)
            result = self.process_item(task)

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
            self.logger.error(f"Executor {self.name}: Exception hit during processing.")
            self.logger.error(e)
            self.logger.error("Traceback:\n%s", traceback.format_exc())

            if "task" in locals():
                self.logger.error(
                    f"Marking task {task.id} as failed and retrying if applicable."
                )
                self.mark_task_failed(task.id)
                self.attempt_retry(task.id)

                on_error = self.task_registry.get_on_error_handler(task.task_name)
                if on_error:
                    try:
                        sig = signature(on_error)
                        if len(sig.parameters) == 0:
                            on_error()
                        else:
                            on_error(e)

                    except Exception as on_error_exception:
                        self.logger.error(
                            "Exception hit when running on_error handler."
                        )
                        self.logger.error(on_error_exception)
                        self.logger.error("Traceback:\n%s", traceback.format_exc())

            self._stop_event.wait(0.5)  # Add delay after error
            return True
        finally:
            self.update_current_task(None)
            clear_hyrex_context()

    def check_root_process(self):
        # Confirm parent is still alive
        if not is_process_alive(self.parent_pid):
            self.logger.warning("Root process died unexpectedly. Shutting down.")
            self._stop_event.set()

    def run_static_queue_loop(self):
        queue = HyrexQueue(
            name=self.queue,
            concurrency_limit=self.task_registry.get_concurrency_limit(
                queue_name=self.queue
            ),
        )
        while not self._stop_event.is_set():
            # Queue pattern is a static string - fetch from it directly.
            if not self.process(queue):
                # No task found, sleep for a bit
                self._stop_event.wait(0.5)
            self.check_root_process()

    def run_round_robin_loop(self):
        while not self._stop_event.is_set():
            self.update_queue_list()

            no_task_count = 0

            while self.queue_list and not self._stop_event.is_set():
                self.check_root_process()
                queue = self.queue_list.pop()
                if not self.process(queue=queue):
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
