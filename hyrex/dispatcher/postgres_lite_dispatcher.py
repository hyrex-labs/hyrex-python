import logging
from uuid import UUID

from psycopg.types.json import Json
from psycopg_pool import ConnectionPool
from uuid_extensions import uuid7

from hyrex import constants, sql
from hyrex.dispatcher.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, StatusEnum


class PostgresLiteDispatcher(Dispatcher):
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        # TODO: Set min/max size based on worker threads
        self.pool = ConnectionPool(conn_string, open=True)

    def mark_success(self, task_id: UUID):
        with self.pool.connection() as conn:
            conn.execute(sql.MARK_TASK_SUCCESS, [task_id])
            conn.commit()

    def mark_failed(self, task_id: UUID):
        with self.pool.connection() as conn:
            conn.execute(sql.MARK_TASK_FAILED, [task_id])
            conn.commit()

    def attempt_retry(self, task_id: UUID):
        with self.pool.connection() as conn:
            conn.execute(
                sql.CONDITIONALLY_RETRY_TASK,
                {"existing_id": task_id, "new_id": uuid7()},
            )
            conn.commit()

    def reset_or_cancel_task(self, task_id: UUID):
        with self.pool.connection() as conn:
            conn.execute(sql.RESET_OR_CANCEL_TASK, [task_id])
            conn.commit()

    def cancel_task(self, task_id: UUID):
        with self.pool.connection() as conn:
            conn.execute(sql.MARK_TASK_CANCELED, [task_id])
            conn.commit()

    def dequeue(
        self,
        worker_id: UUID,
        queue: str = constants.DEFAULT_QUEUE,
        num_tasks: int = 1,
    ) -> list[DequeuedTask]:
        dequeued_tasks = []
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
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

    def enqueue(
        self,
        task: HyrexTask,
    ):
        task_data = (
            task.id,
            task.root_id,
            task.task_name,
            Json(task.args),
            task.queue,
            task.max_retries,
            task.priority,
        )

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    sql.ENQUEUE_TASK,
                    task_data,
                )
                conn.commit()

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        logging.info("Stopping dispatcher...")
        self.pool.close()
        logging.info("Dispatcher stopped successfully!")

    def get_task_status(self, task_id: UUID) -> StatusEnum:
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql.GET_TASK_STATUS, [task_id])
                result = cursor.fetchone()
                if result is None:
                    raise ValueError(f"Task id {task_id} not found in DB.")
                return result[0]

    def register_worker(self, worker_id: UUID, worker_name: str, queue: str):
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql.REGISTER_WORKER, [worker_id, worker_name, queue])
                conn.commit()

    def mark_worker_stopped(self, worker_id: UUID):
        with self.pool.connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql.MARK_WORKER_STOPPED, [worker_id])
                conn.commit()

    def get_workers_to_cancel(self, worker_ids: list[UUID]):
        pass

    def save_result(self, task_id: UUID, result: str):
        pass
