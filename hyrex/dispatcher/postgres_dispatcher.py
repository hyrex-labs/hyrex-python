import logging
import threading
import time
from queue import Empty, Queue
from typing import List
from uuid import UUID

from psycopg.types.json import Json
from psycopg import RawCursor
from psycopg_pool import ConnectionPool
from uuid_extensions import uuid7

from hyrex import constants, sql
from hyrex.dispatcher.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, StatusEnum

# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
# )
# logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.WARNING)


class PostgresDispatcher(Dispatcher):
    def __init__(
        self,
        conn_string: str,
        batch_size=1000,
        flush_interval=0.1,
    ):
        self.conn_string = conn_string
        self.pool = ConnectionPool(
            conn_string, open=True, min_size=2, max_size=10, timeout=30
        )

        self.local_queue = Queue()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._shutdown_event = threading.Event()

        # Non-daemon thread for safe shutdown
        self.thread = threading.Thread(target=self._batch_enqueue, daemon=False)
        self.thread.start()

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

    def enqueue(self, task: HyrexTask):
        # Add backpressure when queue gets too large
        if self.local_queue.qsize() > self.batch_size * 10:
            time.sleep(0.001)  # Small delay to prevent overwhelming
        self.local_queue.put(task)

    def _batch_enqueue(self):
        tasks = []
        last_flush_time = time.monotonic()

        while True:
            current_time = time.monotonic()
            timeout = max(0, self.flush_interval - (current_time - last_flush_time))

            try:
                # Dynamic batch sizing based on queue pressure
                current_batch_size = min(
                    self.batch_size,
                    max(100, self.local_queue.qsize()),  # Minimum batch of 100
                )

                # Collect tasks up to batch size or timeout
                while len(tasks) < current_batch_size and timeout > 0:
                    task = self.local_queue.get(timeout=timeout)
                    if task is None:  # Stop signal
                        if tasks:
                            self._enqueue_tasks(tasks)
                        return
                    tasks.append(task)
                    timeout = max(
                        0, self.flush_interval - (time.monotonic() - last_flush_time)
                    )

                # Flush if we have tasks and either reached batch size or timeout
                if tasks and (len(tasks) >= current_batch_size or timeout <= 0):
                    self._enqueue_tasks(tasks)
                    tasks = []
                    last_flush_time = time.monotonic()

            except Empty:
                if tasks:  # Flush remaining tasks if timeout occurred
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()

    def _enqueue_tasks(self, tasks: List[HyrexTask]):
        start_time = time.monotonic()

        # Prepare the data
        task_data = [
            (
                task.id,
                task.root_id,
                task.task_name,
                Json(task.args),
                task.queue,
                task.max_retries,
                task.priority,
            )
            for task in tasks
        ]
        prep_time = time.monotonic() - start_time

        db_start = time.monotonic()
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                # Temporarily disable synchronous commit for this transaction
                cur.execute("SET LOCAL synchronous_commit TO OFF")

                # Use larger batches for executemany
                # psycopg's executemany uses prepared statements under the hood
                cur.execute("SET LOCAL work_mem = '256MB'")

                cur.executemany(
                    """
                    INSERT INTO hyrextask (
                        id,
                        root_id,
                        task_name,
                        args,
                        queue,
                        max_retries,
                        priority
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    """,
                    task_data,
                )

            conn.commit()

        total_time = time.monotonic() - start_time
        tasks_per_second = len(tasks) / total_time
        logging.info(
            f"Enqueued {len(tasks)} tasks in {total_time:.3f}s "
            f"({tasks_per_second:.1f} tasks/sec)"
        )

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        logging.info("Stopping dispatcher...")
        # Add value to indicate cancellation and unblock the queue
        self.local_queue.put(None)
        self.thread.join()
        # Close the connection pool
        self.pool.close()
        logging.info("Dispatcher stopped successfully!")

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
