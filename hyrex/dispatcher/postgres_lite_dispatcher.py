import random
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID

from psycopg import RawCursor
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool
from uuid_extensions import uuid7

from hyrex import constants, sql
from hyrex.dispatcher.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, StatusEnum


# Single-threaded variant of Postgres dispatcher. (Slower enqueuing.)
class PostgresLiteDispatcher(Dispatcher):
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.pool = ConnectionPool(
            conn_string + "?keepalives=1&keepalives_idle=60&keepalives_interval=10",
            open=True,
            max_idle=300,
        )

        self.register_shutdown_handlers()

    @contextmanager
    def transaction(self):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                try:
                    yield cur
                except InterruptedError:
                    conn.rollback()
                    raise
            conn.commit()

    def mark_success(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.MARK_TASK_SUCCESS, [task_id])

    def mark_failed(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.MARK_TASK_FAILED, [task_id])

    def attempt_retry(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.CONDITIONALLY_RETRY_TASK, [task_id, uuid7()])

    def try_to_cancel_task(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.TRY_TO_CANCEL_TASK, [task_id])

    def task_canceled(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.TASK_CANCELED, [task_id])

    def dequeue(
        self,
        executor_id: UUID,
        queue: str = constants.ANY_QUEUE,
        concurrency_limit=0,
    ) -> DequeuedTask:
        dequeued_task = None
        with self.transaction() as cur:
            if queue == constants.ANY_QUEUE:
                cur.execute(sql.FETCH_TASK_FROM_ANY_QUEUE, [executor_id])
            else:
                if concurrency_limit > 0:
                    cur.execute(
                        sql.FETCH_TASK_WITH_CONCURRENCY,
                        [queue, concurrency_limit, executor_id],
                    )
                else:
                    cur.execute(sql.FETCH_TASK, [queue, executor_id])
            row = cur.fetchone()
            if row:
                (
                    task_id,
                    root_id,
                    parent_id,
                    task_name,
                    args,
                    queue,
                    priority,
                    scheduled_start,
                    queued,
                    started,
                ) = row
                dequeued_task = DequeuedTask(
                    id=task_id,
                    root_id=root_id,
                    parent_id=parent_id,
                    task_name=task_name,
                    args=args,
                    queue=queue,
                    priority=priority,
                    scheduled_start=scheduled_start,
                    queued=queued,
                    started=started,
                )

        return dequeued_task

    def enqueue(
        self,
        task: HyrexTask,
    ):
        task_data = (
            task.id,
            task.root_id,
            task.parent_id,
            task.task_name,
            Json(task.args),
            task.queue,
            task.max_retries,
            task.priority,
        )
        with self.transaction() as cur:
            cur.execute(
                sql.ENQUEUE_TASK,
                task_data,
            )

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        self.logger.debug("Stopping dispatcher...")
        self.pool.close()
        self.logger.debug("Dispatcher stopped successfully!")

    def get_task_status(self, task_id: UUID) -> StatusEnum:
        with self.transaction() as cur:
            cur.execute(sql.GET_TASK_STATUS, [task_id])
            result = cur.fetchone()
            if result is None:
                raise ValueError(f"Task id {task_id} not found in DB.")
            return result[0]

    def register_executor(self, executor_id: UUID, executor_name: str, queue: str):
        with self.transaction() as cur:
            cur.execute(sql.REGISTER_EXECUTOR, [executor_id, executor_name, queue])

    def disconnect_executor(self, executor_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.DISCONNECT_EXECUTOR, [executor_id])

    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        with self.transaction() as cur:
            cur.execute(sql.EXECUTOR_HEARTBEAT, [timestamp, executor_ids])

    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        with self.transaction() as cur:
            cur.execute(sql.TASK_HEARTBEAT, [timestamp, task_ids])

    def get_tasks_up_for_cancel(self) -> list[UUID]:
        with self.transaction() as cur:
            cur.execute(sql.GET_TASKS_UP_FOR_CANCEL)
            return [row[0] for row in cur.fetchall()]

    def mark_running_tasks_lost(self, executor_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.MARK_RUNNING_TASKS_LOST, [executor_id])

    def save_result(self, task_id: UUID, result: str):
        with self.transaction() as cur:
            cur.execute(sql.SAVE_RESULT, [task_id, result])

    def get_queues_for_pattern(self, pattern: str) -> list[str]:
        with self.transaction() as cur:
            cur.execute(sql.GET_UNIQUE_QUEUES_FOR_PATTERN, [pattern])
            return [row[0] for row in cur.fetchall()]
