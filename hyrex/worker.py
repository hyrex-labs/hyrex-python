import asyncio
import json
import logging
import os
import signal
import socket
import threading
import time
import traceback
from datetime import datetime, timezone
from typing import Callable
from uuid import UUID

from pydantic import BaseModel
from uuid_extensions import uuid7

from hyrex.dispatcher import DequeuedTask, get_dispatcher
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.task import TaskWrapper


def generate_worker_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"worker-{hostname}-{pid}-{timestamp}"


class HyrexWorker:

    def __init__(
        self,
        error_callback: Callable = None,
    ):
        self.logger = logging.getLogger(__name__)

        self.name = generate_worker_name()
        self.task_registry: dict[str, TaskWrapper] = {}
        self.dispatcher = get_dispatcher()
        self.error_callback = error_callback

        # For graceful shutdowns
        self.stopping = False
        self._stop_event = threading.Event()

    def set_worker_id(self, worker_id: UUID):
        self.worker_id = worker_id

    def set_queue(self, queue: str):
        self.queue = queue

    def add_registry(self, registry: HyrexRegistry):
        for task_name, task_wrapper in registry.items():
            self.task_registry[task_name] = task_wrapper

    def process_item(self, task_name: str, args: dict):
        task_func = self.task_registry[task_name]
        context = task_func.context_klass(**args)
        result = asyncio.run(task_func.async_call(context))
        return result

    def fetch_task(self) -> list[DequeuedTask]:
        return self.dispatcher.dequeue(worker_id=self.worker_id, queue=self.queue)

    def mark_task_success(self, task_id: UUID):
        self.dispatcher.mark_success(task_id=task_id)

    def mark_task_failed(self, task_id: UUID):
        self.dispatcher.mark_failed(task_id=task_id)

    def attempt_retry(self, task_id: UUID):
        self.dispatcher.attempt_retry(task_id=task_id)

    def reset_or_cancel_task(self, task_id: UUID):
        self.dispatcher.reset_or_cancel_task(task_id=task_id)

    def process(self):
        if self.stopping:
            signal.alarm(0)  # Cancel pending alarm
            raise InterruptedError
        try:
            tasks: list[DequeuedTask] = self.fetch_task()
            if not tasks:
                # No unprocessed items, wait a bit before trying again
                self._stop_event.wait(1.0)
                return

            # TODO: Implement batch processing
            task = tasks[0]
            result = self.process_item(task.name, task.args)

            if result is not None:
                if isinstance(result, BaseModel):
                    result = result.model_dump_json()
                elif isinstance(result, dict):
                    result = json.dumps(result)
                else:
                    raise TypeError("Return value must be JSON-serializable.")

                self.dispatcher.save_result(task.id, result)

            self.mark_task_success(task.id)

            self.logger.info(f"Worker {self.name}: Completed processing item {task.id}")

        except InterruptedError:
            if "task" in locals():
                self.logger.info(
                    f"Worker {self.name}: Processing of item {task.id} was interrupted"
                )
                self.reset_or_cancel_task(task.id)
                self.logger.info(
                    f"Successfully updated task {task.id} on worker {self.name} after interruption"
                )
            raise  # Re-raise the InterruptedError to properly shut down the worker

        except Exception as e:
            self.logger.error(f"Worker {self.name}: Error processing item {str(e)}")
            self.logger.error(e)
            self.logger.error("Traceback:\n%s", traceback.format_exc())
            if self.error_callback:
                task_name = locals().get("task.name", "Unknown task name")
                self.error_callback(task_name, e)

            if "task" in locals():
                self.mark_task_failed(task.id)
                self.attempt_retry(task.id)

            self._stop_event.wait(1.0)  # Add delay after error

    def stop(self):
        self.dispatcher.mark_worker_stopped(worker_id=self.worker_id)
        self.dispatcher.stop()

    def _signal_handler(self, signum, frame):
        self.logger.info("SIGTERM received, stopping worker...")
        self.stopping = True
        self._stop_event.set()  # Wake up from any waiting sleeps
        # Give operations 3 seconds to complete before being interrupted
        signal.alarm(3)

    def _alarm_handler(self, signum, frame):
        raise InterruptedError

    def run(self):
        if not self.worker_id:
            raise RuntimeError("HyrexWorker must have an ID set.")

        if not self.queue:
            raise RuntimeError("HyrexWorker must have a queue set.")

        self.dispatcher.register_worker(
            worker_id=self.worker_id, worker_name=self.name, queue=self.queue
        )

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)
        signal.signal(signal.SIGALRM, self._alarm_handler)

        # Run processing loop
        self.logger.info(f"Worker process {self.name} started - checking for tasks.")
        try:
            while True:
                self.process()
        except InterruptedError:
            self.stop()
            self.logger.info(f"Worker {self.name} stopped.")
