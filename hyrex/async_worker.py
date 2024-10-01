import asyncio
import json
import logging
import os
import random
import re
import signal
import socket
import string
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

T = TypeVar("T", bound=BaseModel)


class UnboundTaskException(Exception):
    """Exception raised for errors in the task binding."""

    def __init__(self, message="Task is unbound."):
        self.message = message
        super().__init__(self.message)


def generate_random_string(length=6):
    return random.choice(string.ascii_lowercase) + "".join(
        random.choices(string.ascii_lowercase + string.digits, k=length - 1)
    )


class TaskRun:
    def __init__(
        self, engine: Engine, task_name: str, task_run_id: str, status: StatusEnum
    ):
        self.task_name = task_name
        self.task_run_id = task_run_id
        self.engine = engine
        self.status = status

    def wait(self, timeout=30, interval=1):
        start = time.time()
        elapsed = 0
        while self.status in [StatusEnum.queued, StatusEnum.running]:
            if elapsed > timeout:
                raise TimeoutError("Waiting for task timed out.")
            with Session(self.engine) as session:
                task_instance = session.get(HyrexTask, self.task_run_id)
                if task_instance is None:
                    raise Exception(
                        "Awaiting a task instance but task id not found in DB."
                    )

                self.status = task_instance.status
            time.sleep(interval)
            elapsed = time.time() - start

    def get_result(self) -> dict:
        with Session(self.engine) as session:
            statement = (
                select(HyrexTaskResult)
                .join(HyrexTask)
                .where(HyrexTask.id == self.task_run_id)
            )
            task_result = session.exec(statement).first()
            if not task_result:
                raise Exception(f"Result not found for task id: {self.task_run_id}")

            return task_result.results

    def __repr__(self):
        return f"TaskRun<{self.task_name}>[{self.task_run_id}]"


class TaskRegistry(dict[str, "TaskWrapper"]):
    def __setitem__(self, key: str, value: "TaskWrapper"):
        if not isinstance(key, str):
            raise TypeError("Key must be an instance of str")
        if not isinstance(value, TaskWrapper):
            raise TypeError("Value must be an instance of TaskWrapper")

        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> "TaskWrapper":
        if not isinstance(key, str):
            raise TypeError("Key must be an instance of str")
        return super().__getitem__(key)


class TaskWrapper(Generic[T]):
    def __init__(
        self,
        task_identifier: str,
        func: Callable[[T], Any],
        queue: str,
        conn: str,
        cron: str | None,
    ):
        self.task_identifier = task_identifier
        self.func = func
        self.queue = queue
        self.conn = conn
        self.engine = create_engine(conn)
        self.signature = signature(func)
        self.type_hints = get_type_hints(func)
        self.cron = cron

        try:
            context_klass = next(iter(self.type_hints.values()))
        except StopIteration:
            raise ValidationError(
                "Hyrex expects all tasks to have 1 arg and for that arg to have a type hint."
            )

        self.context_klass = context_klass
        self._task_instance_id = None

    async def async_call(self, context: T):
        logging.info(f"Executing task {self.func.__name__} on queue: {self.queue}")
        self._check_type(context)
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(context)
        else:
            return self.func(context)

    def __call__(self, context: T):
        self._check_type(context)
        return self.func(context)

    def schedule(self):
        if not self.cron:
            self._unschedule()
            return

        cron_regex = r"(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|([\d\*]+(\/|-)\d+)|\d+|\*) ?){5,7})"
        is_valid = bool(re.fullmatch(cron_regex, self.cron))
        if not is_valid:
            raise ValidationError(f"Cron Expression is not valid: {self.cron}")

        target_db_name = self.conn.split("/")[-1]
        postgres_db = "/".join(self.conn.split("/")[:-1]) + "/postgres"
        with psycopg2.connect(postgres_db) as conn:
            with conn.cursor() as cur:
                sql = f"""
                select
                cron.schedule(
                    '{self.task_identifier}-cron',
                    '{self.cron}',
                    $$INSERT INTO public.hyrextask(id, task_name, status, queue, scheduled_start, started, finished, retried, args) VALUES(gen_random_uuid(), '{self.task_identifier}', 'queued'::statusenum, '{self.queue}', null, null, null, 0, '{{}}');$$
                    );

                UPDATE cron.job SET database = '{target_db_name}' WHERE jobname = '{self.task_identifier}-cron';
                """
                result = cur.execute(sql)
                conn.commit()
                print(f"{self.task_identifier} successfully scheduled.")

    def _unschedule(self):
        postgres_db = "/".join(self.conn.split("/")[:-1]) + "/postgres"
        sql = f"select cron.unschedule('{self.task_identifier}-cron');"
        with psycopg2.connect(postgres_db) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    print(f"Successfully unscheduled {self.task_identifier}")
                except Exception as e:
                    pass
                    # print(f"Unschedule failed with exception {e}")

    def send(self, context: T) -> TaskRun:
        logging.info(f"Sending task {self.func.__name__} to queue: {self.queue}")
        self._check_type(context)

        task = self._enqueue(context)
        logging.info(f"Task sent off to queue: {context}")
        return TaskRun(
            engine=self.engine,
            task_name=self.task_identifier,
            task_run_id=task.id,
            status=task.status,
        )

    def _check_type(self, context: T):
        expected_type = next(iter(self.type_hints.values()))
        try:
            validated_arg = (
                expected_type.parse_obj(context)
                if isinstance(context, dict)
                else expected_type.validate(context)
            )
        except ValidationError as e:
            raise TypeError(
                f"Invalid argument type. Expected {expected_type.__name__}. Error: {e}"
            )

    def _enqueue(self, context: T):
        with Session(self.engine) as session:
            task_instance = HyrexTask(
                task_name=self.task_identifier,
                queue=self.queue,
                args=context.model_dump(),
            )
            session.add(task_instance)
            session.commit()
            session.refresh(task_instance)
            self._task_instance_id = task_instance.id
            return task_instance

    def __repr__(self):
        return f"TaskWrapper<{self.task_identifier}>"


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
        print(f"Creating pool with conn: {self.conn}")
        logging.info(f"Creating pool with conn: {self.conn}")

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
