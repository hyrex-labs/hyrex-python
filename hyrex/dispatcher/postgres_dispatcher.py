import logging
import threading
import time
from queue import Empty, Queue
from typing import List
from uuid import UUID
from statistics import mean, median
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime

from psycopg.types.json import Json
from psycopg import RawCursor
import psycopg
from psycopg_pool import ConnectionPool
from uuid_extensions import uuid7

from hyrex import constants, sql
from hyrex.dispatcher.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, StatusEnum


@dataclass
class BatchMetrics:
    batch_size: int
    enqueue_duration_ms: float
    avg_task_latency_ms: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


class PostgresDispatcher(Dispatcher):
    def __init__(self, conn_string: str, batch_size=5000, flush_interval=0.05):
        self.conn_string = conn_string
        self.pool = ConnectionPool(
            conn_string,
            open=True,
            # check=ConnectionPool.check_connection,
            # kwargs={
            #     "keepalives": 1,
            #     "keepalives_idle": 20,
            #     "keepalives_interval": 10,
            #     "keepalives_count": 5,
            # },
        )

        # Warm up the pool
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

            self.local_queue = Queue()
            self.batch_size = batch_size
            self.flush_interval = flush_interval

        # Metrics tracking
        self.task_enqueue_times = {}  # task_id -> enqueue_start_time
        self.recent_batch_metrics = deque(maxlen=100)  # Keep last 100 batch metrics
        self.metrics_lock = threading.Lock()

        # Configure logging
        self.logger = logging.getLogger("hyrex.dispatcher")
        self.logger.setLevel(logging.INFO)

        # Start the batch enqueue thread
        self.thread = threading.Thread(target=self._batch_enqueue)
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
        start_time = time.perf_counter()
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

        duration_ms = (time.perf_counter() - start_time) * 1000
        self.logger.debug(
            f"Dequeue operation took {duration_ms:.2f}ms for {len(dequeued_tasks)} tasks"
        )
        return dequeued_tasks

    def enqueue(self, task: HyrexTask):
        # Record the time when task enters the queue
        self.task_enqueue_times[task.id] = time.perf_counter()
        self.local_queue.put(task)

    def _batch_enqueue(self):
        tasks = []
        last_flush_time = time.monotonic()
        while True:
            timeout = self.flush_interval - (time.monotonic() - last_flush_time)
            if timeout <= 0:
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()
                continue

            try:
                task = self.local_queue.get(timeout=timeout)
                if task is None:
                    break
                tasks.append(task)
                if len(tasks) >= self.batch_size:
                    self._enqueue_tasks(tasks)
                    tasks = []
                    last_flush_time = time.monotonic()
            except Empty:
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()

        if tasks:
            self._enqueue_tasks(tasks)

    def _enqueue_tasks(self, tasks: List[HyrexTask]):
        """
        Inserts a batch of tasks into the database with timing metrics.
        """
        start_time = time.perf_counter()

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
                cur.executemany(
                    sql.ENQUEUE_TASK,
                    task_data,
                )
            conn.commit()

        end_time = time.perf_counter()
        enqueue_duration_ms = (end_time - start_time) * 1000

        # Calculate individual task latencies
        task_latencies = []
        with self.metrics_lock:
            for task in tasks:
                if task.id in self.task_enqueue_times:
                    latency = (end_time - self.task_enqueue_times[task.id]) * 1000
                    task_latencies.append(latency)
                    del self.task_enqueue_times[task.id]

        # Record batch metrics
        avg_latency = mean(task_latencies) if task_latencies else 0
        batch_metrics = BatchMetrics(
            batch_size=len(tasks),
            enqueue_duration_ms=enqueue_duration_ms,
            avg_task_latency_ms=avg_latency,
        )
        self.recent_batch_metrics.append(batch_metrics)

        # Log the metrics
        self.logger.info(
            f"Batch enqueue metrics: size={len(tasks)}, "
            f"duration={enqueue_duration_ms:.2f}ms, "
            f"avg_latency={avg_latency:.2f}ms"
        )

    def get_metrics_summary(self):
        """
        Returns a summary of recent dispatcher metrics.
        """
        with self.metrics_lock:
            if not self.recent_batch_metrics:
                return {"no_metrics_available": True, "timestamp": datetime.utcnow()}

            batch_sizes = [m.batch_size for m in self.recent_batch_metrics]
            enqueue_durations = [
                m.enqueue_duration_ms for m in self.recent_batch_metrics
            ]
            task_latencies = [m.avg_task_latency_ms for m in self.recent_batch_metrics]

            return {
                "timestamp": datetime.utcnow(),
                "total_batches": len(self.recent_batch_metrics),
                "avg_batch_size": mean(batch_sizes),
                "median_batch_size": median(batch_sizes),
                "avg_enqueue_duration_ms": mean(enqueue_durations),
                "median_enqueue_duration_ms": median(enqueue_durations),
                "avg_task_latency_ms": mean(task_latencies),
                "median_task_latency_ms": median(task_latencies),
                "max_task_latency_ms": max(task_latencies),
                "min_task_latency_ms": min(task_latencies),
            }

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        self.logger.info("Stopping dispatcher...")
        self.local_queue.put(None)
        self.thread.join()
        self.pool.close()
        self.logger.info("Dispatcher stopped successfully!")

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
