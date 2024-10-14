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
from uuid_extensions import uuid7, uuid7str

from hyrex import constants, sql
from hyrex.models import HyrexTask, HyrexWorker, StatusEnum, create_engine
from hyrex.task_registry import TaskRegistry


class WorkerThread(threading.Thread):
    FETCH_TASK_PATH = "/connect/dequeue-task"
    UPDATE_STATUS_PATH = "/connect/update-task-status"

    def __init__(
        self,
        name: str,
        queue: str,
        worker_id: str,
        task_registry: TaskRegistry,
        pg_pool: psycopg_pool.ConnectionPool = None,
        api_key: str = None,
        api_base_url: str = None,
        error_callback: Callable = None,
    ):
        super().__init__(name=name)
        self.loop = asyncio.new_event_loop()
        self._stop_event = asyncio.Event()

        self.queue = queue
        self.worker_id = worker_id
        self.task_registry = task_registry

        self.pool = pg_pool
        self.api_key = api_key
        self.api_base_url = api_base_url

        if not self.pool and not self.api_key:
            raise ValueError(
                "Worker thread must be initialized with either a psycopg2 connection pool, or a platform API key."
            )

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

    async def fetch_task(self):
        if self.api_key:
            # Fetch task using HTTP endpoint
            fetch_url = f"{self.api_base_url}{self.FETCH_TASK_PATH}"
            try:
                async with aiohttp.ClientSession() as session:
                    headers = {"x-project-api-key": self.api_key}
                    json_body = {
                        "queue": self.queue,
                        "worker_id": self.worker_id,
                        "num_tasks": 1,
                    }
                    async with session.post(
                        fetch_url, headers=headers, json=json_body
                    ) as response:
                        if response.status == 200:
                            data = await response.json()
                            if data["tasks"]:
                                task = data["tasks"][0]
                                return task["id"], task["task_name"], task["args"]
                            else:
                                return None
                        else:
                            logging.error(f"Error fetching task: {response.status}")
                            error_body = (
                                await response.text()
                            )  # Get the response body as text
                            logging.error(f"Response body: {error_body}")
                            return None
            except Exception as e:
                logging.error(f"Exception while fetching task: {str(e)}")
                return None
        else:
            # Fetch task from database
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    if self.queue == constants.DEFAULT_QUEUE:
                        cur.execute(sql.FETCH_TASK_FROM_ANY_QUEUE, [self.worker_id])
                    else:
                        cur.execute(sql.FETCH_TASK, [self.queue, self.worker_id])
                    row = cur.fetchone()
                    if row is None:
                        return None
                    return row  # task_id, task_name, args

    async def mark_task_success(self, task_id: str):
        if self.api_key:
            # Use HTTP endpoint to mark task as success
            update_task_url = f"{self.api_base_url}{self.UPDATE_STATUS_PATH}"
            try:
                async with aiohttp.ClientSession() as session:
                    data = {
                        "task_updates": [
                            {"task_id": task_id, "updated_status": "success"}
                        ]
                    }
                    headers = {"x-project-api-key": self.api_key}
                    async with session.post(
                        update_task_url, headers=headers, json=data
                    ) as response:
                        if response.status != 200:
                            logging.error(
                                f"Error marking task success: {response.status}"
                            )
            except Exception as e:
                logging.error(f"Exception while marking task success: {str(e)}")
        else:
            # Update the processed item in a separate transaction
            with self.pool.connection() as conn:
                conn.execute(sql.MARK_TASK_SUCCESS, [task_id])

    async def mark_task_failed(self, task_id: str):
        if self.api_key:
            # Use HTTP endpoint to mark task as failed
            update_task_url = f"{self.api_base_url}{self.UPDATE_STATUS_PATH}"
            try:
                async with aiohttp.ClientSession() as session:
                    data = {
                        "task_updates": [
                            {"task_id": task_id, "updated_status": "failed"}
                        ]
                    }
                    headers = {"x-project-api-key": self.api_key}
                    async with session.post(
                        update_task_url, headers=headers, json=data
                    ) as response:
                        if response.status != 200:
                            logging.error(
                                f"Error marking task failed: {response.status}"
                            )
            except Exception as e:
                logging.error(f"Exception while marking task failed: {str(e)}")
        else:
            with self.pool.connection() as conn:
                conn.execute(sql.MARK_TASK_FAILED, [task_id])

    async def attempt_retry(self, task_id: str):
        # Retrieve task, re-queue it if there are retries left
        if self.api_key:
            raise NotImplementedError("Retries not yet implemented on Hyrex platform")
        else:
            with self.pool.connection() as conn:
                conn.execute(
                    sql.CONDITIONALLY_RETRY_TASK,
                    {"existing_id": task_id, "new_id": uuid7()},
                )

    async def reset_task_status(self, task_id: str):
        if self.api_key:
            # Use HTTP endpoint to reset task status
            update_task_url = f"{self.api_base_url}{self.UPDATE_STATUS_PATH}"
            try:
                async with aiohttp.ClientSession() as session:
                    data = {
                        "task_updates": [
                            {"task_id": task_id, "updated_status": "queued"}
                        ]
                    }
                    headers = {"x-project-api-key": self.api_key}
                    async with session.post(
                        update_task_url, headers=headers, json=data
                    ) as response:
                        if response.status != 200:
                            logging.error(
                                f"Error resetting task status: {response.status}"
                            )
            except Exception as e:
                logging.error(f"Exception while resetting task status: {str(e)}")
        else:
            with self.pool.connection() as conn:
                conn.execute(sql.MARK_TASK_QUEUED, [task_id])

    async def process(self):
        try:
            task = await self.fetch_task()
            if task is None:
                # No unprocessed items, wait a bit before trying again
                await asyncio.sleep(1)
                return

            task_id, task_name, args = task
            await self.process_item(task_name, args)
            await self.mark_task_success(task_id)

            logging.info(f"Worker {self.name}: Completed processing item {task_id}")

        except asyncio.CancelledError:
            if "task_id" in locals():
                logging.info(
                    f"Worker {self.name}: Processing of item {task_id} was interrupted"
                )
                await self.reset_task_status(task_id)
                logging.info(f"Successfully reset task on worker {self.name}")
            raise  # Re-raise the CancelledError to properly shut down the worker

        except Exception as e:
            logging.error(f"Worker {self.name}: Error processing item {str(e)}")
            logging.error(e)
            logging.error("Traceback:\n%s", traceback.format_exc())
            if self.error_callback:
                task_name = locals().get("task_name", "Unknown task name")
                self.error_callback(task_name, e)

            if "task_id" in locals():
                await self.mark_task_failed(task_id)
                await self.attempt_retry(task_id)

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
        conn: str,
        api_key: str,
        api_base_url: str,
        task_registry: TaskRegistry,
        name: str = None,
        num_threads: int = 8,
        error_callback: Callable = None,
    ):
        self.id = uuid7str()
        self.queue = queue
        self.conn = conn
        self.api_key = api_key
        self.api_base_url = api_base_url
        self.pool = None
        self.num_threads = num_threads
        self.threads = []
        self.task_registry = task_registry
        self.error_callback = error_callback
        self.name = name or generate_worker_name()

        if self.api_key:
            logging.info("Running Hyrex worker, connecting to HyrexCloud via API key.")
        elif self.conn:
            logging.info(
                "Running Hyrex worker, connecting to database via connection string."
            )
        else:
            raise KeyError(
                "Attempted to create Hyrex worker without API key or connection string set."
            )

    def connect(self):
        if self.api_key:
            return

        logging.info("Creating worker pool.")

        self.pool = psycopg_pool.ConnectionPool(
            self.conn,
            min_size=max(1, self.num_threads // 4),
            max_size=max(1, self.num_threads // 2),
        )
        logging.info(f"Pool {self.pool}")

    def close(self):
        if self.api_key:
            return

        logging.info("Calling close...")
        self.pool.close()

    def signal_handler(self, signum, frame):
        logging.info("SIGTERM received, stopping all threads...")
        for thread in self.threads:
            thread.stop()

    def _add_to_db(self):
        if self.api_key:
            # Register in app?
            return
        engine = create_engine(self.conn)
        with Session(engine) as session:
            worker = HyrexWorker(id=self.id, name=self.name, queue=self.queue)
            session.add(worker)
            session.commit()

    def _set_stopped_time(self):
        if self.api_key:
            # Register in app?
            return
        engine = create_engine(self.conn)
        with Session(engine) as session:
            worker = session.get(HyrexWorker, self.id)
            worker.stopped = datetime.now(timezone.utc)
            session.add(worker)
            session.commit()

    def run(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self.signal_handler)

        self.connect()
        self._add_to_db()

        self.threads = [
            WorkerThread(
                name=f"WorkerThread{i}",
                worker_id=self.id,
                pg_pool=self.pool,
                api_key=self.api_key,
                api_base_url=self.api_base_url,
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

        # Clean up
        self.close()
        self._set_stopped_time()
        logging.info("All worker threads have been successfully exited!")
