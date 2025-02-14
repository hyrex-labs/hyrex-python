import random
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID

from psycopg import RawCursor
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool
from uuid_extensions import uuid7

from hyrex import constants
from hyrex.dispatcher.dispatcher import (
    DequeuedTask,
    EnqueueTaskRequest,
    TaskStatus,
)
from hyrex.dispatcher.postgres_dispatcher import PostgresDispatcher
from hyrex.hyrex_queue import HyrexQueue
from hyrex.sql import sql


# Single-threaded variant of Postgres dispatcher. (Slower enqueuing.)
class PostgresLiteDispatcher(PostgresDispatcher):
    def __init__(self, conn_string: str):
        self.conn_string = conn_string
        self.pool = ConnectionPool(
            conn_string + "?keepalives=1&keepalives_idle=60&keepalives_interval=10",
            open=True,
            max_idle=5,
        )

        self.register_shutdown_handlers()

    def enqueue(
        self,
        task: EnqueueTaskRequest,
    ):
        task_data = (
            task.id,
            task.durable_id,
            task.root_id,
            task.parent_id,
            task.task_name,
            Json(task.args),
            task.queue,
            task.max_retries,
            task.priority,
            task.timeout_seconds,
            task.idempotency_key,
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
