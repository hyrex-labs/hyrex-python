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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class PostgresDispatcher:
    def __init__(
        self,
        conn_string: str,
        batch_size=1000,
        flush_interval=0.1,
        num_writer_threads=3,  # Multiple writer threads
    ):
        self.conn_string = conn_string
        # Increase pool size for multiple writers
        self.pool = ConnectionPool(
            conn_string,
            open=True,
            min_size=num_writer_threads + 2,  # Account for other operations
            max_size=num_writer_threads + 5,
            timeout=30,
        )

        # Use multiple queues to reduce contention
        self.local_queues = [Queue() for _ in range(num_writer_threads)]
        self.next_queue = 0  # For round-robin distribution
        self.queue_lock = threading.Lock()

        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._shutdown_event = threading.Event()

        # Launch multiple writer threads
        self.threads = []
        for i in range(num_writer_threads):
            thread = threading.Thread(
                target=self._batch_enqueue, args=(i, self.local_queues[i]), daemon=False
            )
            thread.start()
            self.threads.append(thread)

    def enqueue(self, task: HyrexTask):
        # Round-robin distribution across queues
        with self.queue_lock:
            queue_idx = self.next_queue
            self.next_queue = (self.next_queue + 1) % len(self.local_queues)

        queue = self.local_queues[queue_idx]
        if queue.qsize() > self.batch_size * 5:  # Reduced buffer for faster feedback
            time.sleep(0.0001)  # Shorter sleep
        queue.put(task)

    def _batch_enqueue(self, thread_id: int, queue: Queue):
        tasks = []
        last_flush_time = time.monotonic()

        while True:
            try:
                current_time = time.monotonic()
                timeout = max(0, self.flush_interval - (current_time - last_flush_time))

                # Adaptive batch sizing
                current_batch_size = min(
                    self.batch_size,
                    max(250, queue.qsize()),  # Increased minimum batch
                )

                while len(tasks) < current_batch_size and timeout > 0:
                    task = queue.get(timeout=timeout)
                    if task is None:
                        if tasks:
                            self._enqueue_tasks(tasks)
                        return
                    tasks.append(task)
                    timeout = max(
                        0, self.flush_interval - (time.monotonic() - last_flush_time)
                    )

                if tasks and (len(tasks) >= current_batch_size or timeout <= 0):
                    self._enqueue_tasks(tasks)
                    tasks = []
                    last_flush_time = time.monotonic()

            except Empty:
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()

    def _enqueue_tasks(self, tasks: List[HyrexTask]):
        start_time = time.monotonic()

        # Pre-process task data outside the DB connection
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

        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                # Performance optimizations
                cur.execute(
                    """
                    SET LOCAL synchronous_commit TO OFF;
                    SET LOCAL maintenance_work_mem = '1GB';
                    SET LOCAL temp_buffers = '1GB';
                    SET LOCAL work_mem = '256MB';
                """
                )

                # Use UNLOGGED table if possible for better insert performance
                with cur.copy(
                    """
                    COPY hyrextask (
                        id, root_id, task_name, args, queue, max_retries, priority
                    ) FROM STDIN
                    """
                ) as copy:
                    for task in task_data:
                        copy.write_row(task)

            conn.commit()

        total_time = time.monotonic() - start_time
        logger.info(
            f"Enqueued {len(tasks)} tasks in {total_time:.3f}s "
            f"({len(tasks)/total_time:.1f} tasks/sec)"
        )

    def stop(self):
        logging.info("Stopping dispatcher...")
        for queue in self.local_queues:
            queue.put(None)
        for thread in self.threads:
            thread.join()
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
