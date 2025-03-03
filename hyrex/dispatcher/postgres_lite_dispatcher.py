from psycopg.types.json import Json
from psycopg_pool import ConnectionPool

from hyrex.dispatcher.postgres_dispatcher import PostgresDispatcher
from hyrex.hyrex_queue import HyrexQueue
from hyrex.schemas import EnqueueTaskRequest
from hyrex.sql import cron_sql, sql


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
        tasks: list[EnqueueTaskRequest],
    ):
        task_data = (
            (
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
                task.status,
                task.workflow_run_id,
                task.workflow_dependencies,
            )
            for task in tasks
        )

        with self.transaction() as cur:
            cur.executemany(
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
