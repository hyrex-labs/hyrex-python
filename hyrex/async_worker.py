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
from typing import Any, Callable, Generic, TypeVar, get_type_hints
from uuid import UUID

import aiohttp
import psycopg2
import psycopg_pool
from pydantic import BaseModel, ValidationError
from sqlalchemy import Engine
from sqlmodel import Session, select
from uuid_extensions import uuid7

from hyrex import constants, sql
from hyrex.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, HyrexWorker, StatusEnum, create_engine
from hyrex.task_registry import TaskRegistry


class WorkerThread(threading.Thread):
    # FETCH_TASK_PATH = "/connect/dequeue-task"
    # UPDATE_STATUS_PATH = "/connect/update-task-status"

    def __init__(
        self,
        name: str,
        queue: str,
        worker_id: str,
        task_registry: TaskRegistry,
        dispatcher: Dispatcher,
        num_tasks: int = 1,
        error_callback: Callable = None,
    ):
        super().__init__(name=name)
        self.loop = asyncio.new_event_loop()
        self._stop_event = asyncio.Event()

        self.queue = queue
        self.worker_id = worker_id
        self.task_registry = task_registry
        self.num_tasks = num_tasks

        self.dispatcher = dispatcher

        self.error_callback = error_callback

        self.current_asyncio_task = None

    def run(self):
        asyncio.set_event_loop(self.loop)
        asyncio.run(self.processing_loop())

    async def process_item(self, task_name: str, args: dict):
        task_func = self.task_registry[task_name]
        context = task_func.context_klass(**args)
        result = await task_func.async_call(context)
        return result

    async def fetch_tasks(self) -> list[DequeuedTask]:
        return self.dispatcher.dequeue(
            worker_id=self.worker_id, queue=self.queue, num_tasks=self.num_tasks
        )

    async def mark_task_success(self, task_id: UUID):
        self.dispatcher.mark_success(task_id=task_id)

    async def mark_task_failed(self, task_id: UUID):
        self.dispatcher.mark_failed(task_id=task_id)

    async def attempt_retry(self, task_id: UUID):
        self.dispatcher.attempt_retry(task_id=task_id)

    async def reset_or_cancel_task(self, task_id: UUID):
        self.dispatcher.reset_or_cancel_task(task_id=task_id)

    async def process(self):
        try:
            tasks: DequeuedTask = await self.fetch_tasks()
            if not tasks:
                # No unprocessed items, wait a bit before trying again
                await asyncio.sleep(1)
                return

            # TODO: Implement batch processing
            task = tasks[0]
            await self.process_item(task.name, task.args)
            await self.mark_task_success(task.id)

            logging.info(f"Worker {self.name}: Completed processing item {task.id}")

        except asyncio.CancelledError:
            if "task" in locals():
                logging.info(
                    f"Worker {self.name}: Processing of item {task.id} was interrupted"
                )
                await self.reset_or_update_task(task.id)
                logging.info(f"Successfully reset task on worker {self.name}")
            raise  # Re-raise the CancelledError to properly shut down the worker

        except Exception as e:
            logging.error(f"Worker {self.name}: Error processing item {str(e)}")
            logging.error(e)
            logging.error("Traceback:\n%s", traceback.format_exc())
            if self.error_callback:
                task_name = locals().get("task.name", "Unknown task name")
                self.error_callback(task_name, e)

            if "task" in locals():
                await self.mark_task_failed(task.id)
                await self.attempt_retry(task.id)

            await asyncio.sleep(1)  # Add delay after error

    async def processing_loop(self):
        try:
            while not self._stop_event.is_set():
                self.current_asyncio_task = asyncio.create_task(self.process())
                await self.current_asyncio_task
        except asyncio.CancelledError:
            logging.info(f"Worker thread {self.name} was cancelled.")

    def stop(self):
        logging.info(f"{self.name} received stop signal.")

        self._stop_event.set()
        if self.current_asyncio_task:
            self.current_asyncio_task.cancel()
        else:
            logging.info("No current task to be found")


def generate_worker_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"worker-{hostname}-{pid}-{timestamp}"


class AsyncWorker:

    def __init__(
        self,
        queue: str,
        dispatcher: Dispatcher,
        task_registry: TaskRegistry,
        name: str = None,
        num_threads: int = 8,
        error_callback: Callable = None,
    ):
        self.id = uuid7()
        self.queue = queue
        self.dispatcher = dispatcher
        self.num_threads = num_threads
        self.threads = []
        self.task_registry = task_registry
        self.error_callback = error_callback
        self.name = name or generate_worker_name()

    def close(self):
        pass

    def _signal_handler(self, signum, frame):
        logging.info("SIGTERM received, stopping all threads...")
        for thread in self.threads:
            thread.stop()

    def run(self):
        # Note: This overrides the Hyrex instance signal handler,
        # which makes the async_worker responsible for stopping the dispatcher.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

        self.dispatcher.register_worker(self.id, self.name, self.queue)

        self.threads = [
            WorkerThread(
                name=f"WorkerThread{i}",
                worker_id=self.id,
                dispatcher=self.dispatcher,
                queue=self.queue,
                task_registry=self.task_registry,
                error_callback=self.error_callback,
            )
            for i in range(self.num_threads)
        ]

        # Kick off all worker threads
        logging.info("Starting worker threads.")
        for thread in self.threads:
            thread.start()

        # Wait for them to finish
        for thread in self.threads:
            thread.join()

        logging.info("All worker threads have been successfully exited!")

        self.dispatcher.mark_worker_stopped(worker_id=self.id)
        self.dispatcher.stop()
