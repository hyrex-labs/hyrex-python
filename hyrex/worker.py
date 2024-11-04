import asyncio
import json
import logging
import os
import signal
import socket
import traceback
from datetime import datetime, timezone
from typing import Callable, Dict
from uuid import UUID

from pydantic import BaseModel

from hyrex.dispatcher import get_dispatcher, DequeuedTask
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.task import TaskWrapper


def generate_worker_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"worker-{hostname}-{pid}-{timestamp}"


class HyrexWorker:

    def __init__(self, error_callback: Callable = None):
        self.name = generate_worker_name()
        self.task_registry: Dict[str, TaskWrapper] = {}
        self.dispatcher = get_dispatcher()
        self.error_callback = error_callback
        self.worker_id = None
        self.queue = None
        self.shutdown_event = asyncio.Event()

    def set_worker_id(self, worker_id: UUID):
        self.worker_id = worker_id

    def set_queue(self, queue: str):
        self.queue = queue

    def add_registry(self, registry: HyrexRegistry):
        for task_name, task_wrapper in registry.items():
            self.task_registry[task_name] = task_wrapper

    async def process_item(self, task_name: str, args: dict):
        task_func = self.task_registry[task_name]
        context = task_func.context_klass(**args)
        result = await task_func.async_call(context)
        return result

    def fetch_task(self) -> DequeuedTask:
        tasks = self.dispatcher.dequeue(worker_id=self.worker_id, queue=self.queue)
        if tasks:
            return tasks[0]
        else:
            return None

    def mark_task_success(self, task_id: UUID):
        self.dispatcher.mark_success(task_id=task_id)

    def mark_task_failed(self, task_id: UUID):
        self.dispatcher.mark_failed(task_id=task_id)

    def attempt_retry(self, task_id: UUID):
        self.dispatcher.attempt_retry(task_id=task_id)

    def reset_or_cancel_task(self, task_id: UUID):
        self.dispatcher.reset_or_cancel_task(task_id=task_id)

    async def process(self):
        task = None
        try:
            task = self.fetch_task()
            if not task:
                await asyncio.sleep(1)
                return

            # Process the task
            result = await self.process_task(task)
            await self.handle_successful_task(task, result)

        except asyncio.CancelledError:
            if task:
                logging.info(
                    f"Worker {self.name}: Processing of item {task.id} was cancelled"
                )
                self.reset_or_cancel_task(task.id)
            raise
        except Exception as e:
            logging.error(f"Worker {self.name}: Error processing task: {e}")
            logging.error("Traceback:\n%s", traceback.format_exc())

            if self.error_callback:
                task_name = task.name if task else "Unknown task"
                self.error_callback(task_name, e)

            if task:
                self.mark_task_failed(task.id)
                self.attempt_retry(task.id)

            await asyncio.sleep(1)  # Add delay after error

    async def process_task(self, task: DequeuedTask):
        try:
            result = await self.process_item(task.name, task.args)
            return result
        except asyncio.CancelledError:
            # Task was cancelled
            logging.info(f"Task {task.id} was cancelled during processing")
            self.reset_or_cancel_task(task.id)
            raise
        except Exception as e:
            # Reraise exception to be handled by the caller
            raise e

    async def handle_successful_task(self, task: DequeuedTask, result):
        if result is not None:
            if isinstance(result, BaseModel):
                result = result.model_dump_json()
            elif isinstance(result, dict):
                result = json.dumps(result)
            else:
                raise TypeError("Return value must be JSON-serializable.")

            self.dispatcher.save_result(task.id, result)

        self.mark_task_success(task.id)
        logging.info(f"Worker {self.name}: Completed processing item {task.id}")

    def run(self):
        if not self.worker_id:
            raise RuntimeError("HyrexWorker must have an ID set.")

        if not self.queue:
            raise RuntimeError("HyrexWorker must have a queue set.")

        self.dispatcher.register_worker(
            worker_id=self.worker_id, worker_name=self.name, queue=self.queue
        )

        logging.info(f"Worker process {self.name} started - checking for tasks.")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))

        try:
            loop.run_until_complete(self._run_async())
        finally:
            loop.close()

    async def _run_async(self):
        while not self.shutdown_event.is_set():
            await self.process()

    async def shutdown(self):
        logging.info(f"Worker {self.name} received shutdown signal. Shutting down.")
        self.shutdown_event.set()
        self.stop()

    def stop(self):
        self.dispatcher.mark_worker_stopped(worker_id=self.worker_id)
        self.dispatcher.stop()
