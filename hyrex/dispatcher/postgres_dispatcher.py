import json
import logging
import threading
import time
from queue import Empty, Queue
from typing import List
from uuid import UUID

import psycopg
from psycopg import RawCursor
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool
from uuid_extensions import uuid7

from hyrex import constants, sql
from hyrex.dispatcher.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, StatusEnum


class PostgresDispatcher(Dispatcher):
    def __init__(self, conn_string: str, num_workers: int = 2, batch_size: int = 20):
        self.pool = ConnectionPool(
            conn_string, min_size=num_workers, max_size=num_workers, open=True
        )
        self.queue = Queue()
        self.batch_size = batch_size
        self.stop_flag = threading.Event()

        self.workers = []
        for i in range(num_workers):
            worker = threading.Thread(target=self._worker_routine, args=(i,))
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def _worker_routine(self, worker_id: int):
        tasks = []
        while not self.stop_flag.is_set() or not self.queue.empty():
            try:
                try:
                    task = self.queue.get(timeout=0.1)
                    tasks.append(task)
                except Empty:
                    if tasks:
                        self._process_batch(tasks)
                        tasks = []
                    continue

                if len(tasks) >= self.batch_size:
                    self._process_batch(tasks)
                    tasks = []

            except Exception as e:
                print(f"Worker {worker_id} error: {e}")
                if tasks:
                    try:
                        self._process_batch(tasks)
                    except:
                        pass
                    tasks = []

    def _process_batch(self, tasks: List["HyrexTask"]):
        with self.pool.connection() as conn:
            try:
                with conn.cursor() as cur:
                    task_data = [
                        (
                            str(task.id),
                            str(task.root_id),
                            task.task_name,
                            json.dumps(task.args),
                            task.queue,
                            task.max_retries,
                            task.priority,
                        )
                        for task in tasks
                    ]
                    cur.executemany(
                        """
                        INSERT INTO hyrextask (
                            id, root_id, task_name, args, queue,
                            max_retries, priority
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                        task_data,
                    )
                    conn.commit()
            except Exception as e:
                conn.rollback()
                raise e

    def enqueue(self, task: "HyrexTask"):
        self.queue.put(task)

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        self.stop_flag.set()
        self.queue.put(None)
        for worker in self.workers:
            worker.join()
        self.pool.close()

    def mark_success(self, task_id: UUID):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.MARK_TASK_SUCCESS, [task_id])
            conn.commit()

    def mark_failed(self, task_id: UUID):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.MARK_TASK_FAILED, [task_id])
            conn.commit()

    def attempt_retry(self, task_id: UUID):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(
                    sql.CONDITIONALLY_RETRY_TASK,
                    [task_id, uuid7()],
                )
            conn.commit()

    def reset_or_cancel_task(self, task_id: UUID):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.RESET_OR_CANCEL_TASK, [task_id])
            conn.commit()

    def cancel_task(self, task_id: UUID):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.MARK_TASK_CANCELED, [task_id])
            conn.commit()

    def dequeue(
        self,
        worker_id: UUID,
        queue: str = constants.DEFAULT_QUEUE,
        num_tasks: int = 1,
    ) -> list[DequeuedTask]:
        dequeued_tasks = []
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                if queue == constants.DEFAULT_QUEUE:
                    cur.execute(sql.FETCH_TASK_FROM_ANY_QUEUE, [worker_id])
                else:
                    cur.execute(sql.FETCH_TASK, [queue, worker_id])
                row = cur.fetchone()
                if row:
                    task_id, task_name, task_args = row
                    dequeued_tasks.append(
                        DequeuedTask(id=task_id, name=task_name, args=task_args)
                    )
        return dequeued_tasks

    def get_task_status(self, task_id: UUID) -> StatusEnum:
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.GET_TASK_STATUS, [task_id])
                result = cur.fetchone()
                if result is None:
                    raise ValueError(f"Task id {task_id} not found in DB.")
                return result[0]

    def register_worker(self, worker_id: UUID, worker_name: str, queue: str):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.REGISTER_WORKER, [worker_id, worker_name, queue])
            conn.commit()

    def mark_worker_stopped(self, worker_id: UUID):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.MARK_WORKER_STOPPED, [worker_id])
            conn.commit()

    def get_workers_to_cancel(self, worker_ids: list[UUID]) -> list[UUID]:
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.GET_WORKERS_TO_CANCEL, (worker_ids,))
                result = cur.fetchall()
                return [row[0] for row in result]

    def save_result(self, task_id: UUID, result: str):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                cur.execute(sql.SAVE_RESULT, [task_id, result])
                conn.commit()
