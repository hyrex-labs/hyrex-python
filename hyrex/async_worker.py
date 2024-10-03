import asyncio
import json
import logging
import os
import random
import re
import signal
import socket
import string
import sys
import threading
import time
import traceback
from datetime import datetime, timezone
from inspect import signature
from typing import Any, Callable, Generic, TypeVar, get_type_hints

import psycopg2
import psycopg_pool
from pydantic import BaseModel, ValidationError
from sqlalchemy import Engine
from sqlmodel import Session, select
from uuid_extensions import uuid7str

from hyrex import sql
from hyrex.models import (
    HyrexTask,
    HyrexTaskResult,
    HyrexWorker,
    StatusEnum,
    create_engine,
)
from hyrex.task_registry import TaskRegistry


class WorkerThread(threading.Thread):
    def __init__(
        self,
        name: str,
        queue: str,
        worker_id: str,
        pg_pool: psycopg_pool.ConnectionPool,
        task_registry: TaskRegistry,
        error_callback: Callable = None,
    ):
        super().__init__(name=name)
        self.loop = asyncio.new_event_loop()
        self._stop_event = asyncio.Event()

        self.queue = queue
        self.worker_id = worker_id
        self.pool = pg_pool
        self.task_registry = task_registry
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

    async def process(self):
        # Select and lock 1 unprocessed row
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cur:
                    if self.queue == "default":
                        cur.execute(sql.FETCH_TASK_FROM_ANY_QUEUE, [self.worker_id])
                    else:
                        cur.execute(sql.FETCH_TASK, [self.queue, self.worker_id])
                    row = cur.fetchone()
                    if row is None:
                        # No unprocessed items, wait a bit before trying again
                        time.sleep(1)
                        return

            task_id, task_name, args = row
            # item = TaskItem(**data)
            result = await self.process_item(task_name, args)

            if result is not None:
                if isinstance(result, BaseModel):
                    result = result.model_dump_json()
                elif isinstance(result, dict):
                    result = json.dumps(result)
                else:
                    raise TypeError("Return value must be JSON-serializable.")

                with self.pool.connection() as conn:
                    conn.execute(sql.SAVE_RESULTS, [task_id, result])

            # Update the processed item in a separate transaction
            with self.pool.connection() as conn:
                conn.execute(sql.MARK_TASK_SUCCESS, [task_id])

            logging.info(f"Worker {self.name}: Completed processing item {task_id}")

        except asyncio.CancelledError:
            logging.info(
                f"Worker {self.name}: Processing of item {task_id} was interrupted"
            )
            # Update the item status back to 'queued' so it can be picked up again later
            with self.pool.connection() as conn:
                conn.execute(sql.MARK_TASK_QUEUED, [task_id])

            logging.info(f"Successfully reset task on work {self.name}")
            raise  # re-raise the CancelledError to properly shut down the worker

        except Exception as e:
            logging.error(f"Worker {self.name}: Error processing item {str(e)}")
            logging.error(e)
            logging.error("Traceback:\n%s", traceback.format_exc())
            if self.error_callback:
                if "task_name" in locals():
                    self.error_callback(task_name, e)
                else:
                    self.error_callback("Unknown task name", e)

            if "task_id" in locals():
                with self.pool.connection() as conn:
                    conn.execute(sql.MARK_TASK_FAILED, [task_id])

            await asyncio.sleep(1)  # Add delay after error
            # raise

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
        task_registry: TaskRegistry,
        name: str = None,
        num_threads: int = 8,
        error_callback: Callable = None,
    ):
        self.id = uuid7str()
        self.queue = queue
        self.conn = conn
        self.pool = None
        self.num_threads = num_threads
        self.threads = []
        self.task_registry = task_registry
        self.error_callback = error_callback
        self.name = name or generate_worker_name()

    def connect(self):
        logging.info("Creating worker pool.")

        self.pool = psycopg_pool.ConnectionPool(
            self.conn,
            min_size=max(1, self.num_threads // 4),
            max_size=max(1, self.num_threads // 2),
        )
        logging.info(f"Pool {self.pool}")

    def close(self):
        logging.info("Calling close...")
        self.pool.close()

    def signal_handler(self, signum, frame):
        logging.info("SIGTERM received, stopping all threads...")
        for thread in self.threads:
            thread.stop()

    def _add_to_db(self):
        engine = create_engine(self.conn)
        with Session(engine) as session:
            worker = HyrexWorker(id=self.id, name=self.name, queue=self.queue)
            session.add(worker)
            session.commit()

    def _set_stopped_time(self):
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
                queue=self.queue,
                task_registry=self.task_registry,
                error_callback=self.error_callback,
            )
            for i in range(self.num_threads)
        ]

        # Kick off all worker threads
        for thread in self.threads:
            thread.start()

        # Wait for them to finish
        for thread in self.threads:
            thread.join()

        # Clean up
        self.close()
        self._set_stopped_time()
        logging.info("All worker threads have been successfully exited!")
