import asyncio
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

from uuid_extensions import uuid7

from hyrex.dispatcher import DequeuedTask, Dispatcher
from hyrex.task_registry import TaskRegistry


def generate_worker_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"worker-{hostname}-{pid}-{timestamp}"


class Worker:

    def __init__(
        self,
        queue: str,
        worker_id: UUID,
        task_registry: TaskRegistry,
        dispatcher: Dispatcher,
        error_callback: Callable = None,
    ):
        # super().__init__(name=name)
        self.name = generate_worker_name()

        self.queue = queue
        self.worker_id = worker_id
        self.task_registry = task_registry

        self.dispatcher = dispatcher

        self.error_callback = error_callback

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
        try:
            tasks: list[DequeuedTask] = self.fetch_task()
            if not tasks:
                # No unprocessed items, wait a bit before trying again
                time.sleep(1)
                return

            # TODO: Implement batch processing
            task = tasks[0]
            self.process_item(task.name, task.args)
            self.mark_task_success(task.id)

            logging.info(f"Worker {self.name}: Completed processing item {task.id}")

        except InterruptedError:
            if "task" in locals():
                logging.info(
                    f"Worker {self.name}: Processing of item {task.id} was interrupted"
                )
                self.reset_or_cancel_task(task.id)
                logging.info(
                    f"Successfully updated task {task.id} on worker {self.name} after interruption"
                )
            raise  # Re-raise the CancelledError to properly shut down the worker

        except Exception as e:
            logging.error(f"Worker {self.name}: Error processing item {str(e)}")
            logging.error(e)
            logging.error("Traceback:\n%s", traceback.format_exc())
            if self.error_callback:
                task_name = locals().get("task.name", "Unknown task name")
                self.error_callback(task_name, e)

            if "task" in locals():
                self.mark_task_failed(task.id)
                self.attempt_retry(task.id)

            time.sleep(1)  # Add delay after error

    def stop(self):
        self.dispatcher.mark_worker_stopped(worker_id=self.worker_id)
        self.dispatcher.stop()

    def _signal_handler(self, signum, frame):
        logging.info("SIGTERM received, stopping worker...")
        raise InterruptedError

    def run(self):
        self.dispatcher.register_worker(
            worker_id=self.worker_id, worker_name=self.name, queue=self.queue
        )

        # Note: This overrides the Hyrex instance signal handler,
        # which makes the worker responsible for stopping the dispatcher.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

        # Run processing loop
        logging.info(f"Worker process {self.name} started - checking for tasks.")
        try:
            while True:
                self.process()
        except InterruptedError:
            self.stop()
            logging.info(f"Worker {self.name} stopped.")
