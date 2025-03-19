import asyncio
import logging
import multiprocessing

import psycopg  # Added import for database connections
import pytest
from pydantic import BaseModel

from hyrex.constants import DEFAULT_QUEUE
from hyrex.dispatcher import get_dispatcher
from hyrex.dispatcher.postgres_dispatcher import PostgresDispatcher
from hyrex.hyrex_app import HyrexApp
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.init_db import create_tables
from hyrex.worker.root_process import run_worker

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class EmptyContext(BaseModel):
    pass


class NumberContext(BaseModel):
    number: int


def register_tasks(registry: HyrexRegistry):
    @registry.task
    def empty_task(context: EmptyContext):
        print("Completed empty task.")

    @registry.task
    def error_task(context: EmptyContext):
        raise RuntimeError("This task has raised an error.")

    @registry.task
    def number_task(context: NumberContext):
        print(f"Received number {context.number}")

    return empty_task, error_task, number_task


def worker_process(db_connection_string):
    import os

    from hyrex.env_vars import EnvVars

    # Set worker process environment variable
    os.environ[EnvVars.WORKER_PROCESS] = "1"
    os.environ[EnvVars.DATABASE_URL] = db_connection_string

    # Create a HyrexApp for the worker
    app = HyrexApp(app_name="hyrex-smoke-test")
    registry = HyrexRegistry(queue=DEFAULT_QUEUE)
    register_tasks(registry)
    app.add_registry(registry)

    # Run the worker
    run_worker(app_name="hyrex-smoke-test", dispatcher_type="postgres")


@pytest.fixture
def db_connection_string(postgresql):
    db_info = postgresql.info

    connection_string = (
        f"postgresql://{db_info.user}:{db_info.password}"
        f"@{db_info.host}:{db_info.port}/{db_info.dbname}"
    )
    return connection_string


def clear_db(db_connection_string: str):
    conn = psycopg.connect(db_connection_string)
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM hyrextask;")
            conn.commit()
    finally:
        conn.close()


def get_completed_tasks(db_connection_string: str):
    conn = psycopg.connect(db_connection_string)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM hyrextask WHERE status='success';")
            result = cursor.fetchone()
            return result[0] if result else 0
    finally:
        conn.close()


def get_failed_tasks(db_connection_string: str):
    conn = psycopg.connect(db_connection_string)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM hyrextask WHERE status='failed';")
            result = cursor.fetchone()
            return result[0] if result else 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_hyrex(db_connection_string):
    logger.info("Creating tables...")
    create_tables(db_connection_string)

    # Create a dispatcher for sending tasks
    dispatcher = PostgresDispatcher(
        conn_string=db_connection_string, app_name="hyrex-smoke-test"
    )

    # Create a registry with the dispatcher
    registry = HyrexRegistry(queue=DEFAULT_QUEUE)
    registry.set_dispatcher(dispatcher)

    # Register tasks
    empty_task, error_task, number_task = register_tasks(registry)

    # Run worker in a separate process
    ctx = multiprocessing.get_context("spawn")
    worker = ctx.Process(target=worker_process, args=(db_connection_string,))
    worker.start()

    # Validate queueing/running of tasks
    try:
        # Single task is pulled off queue
        empty_task.send(EmptyContext())
        await asyncio.sleep(1)
        assert get_completed_tasks(db_connection_string) == 1
        clear_db(db_connection_string)

        # Task with errors is retried
        error_task.with_config(max_retries=3).send(EmptyContext())
        await asyncio.sleep(5)
        assert get_failed_tasks(db_connection_string) == 4  # Original + 3 retries
        clear_db(db_connection_string)

        # Many tasks queued/run
        for i in range(1, 20):
            number_task.send(NumberContext(number=i))
        await asyncio.sleep(3)
        assert get_completed_tasks(db_connection_string) == 19
        clear_db(db_connection_string)

    finally:
        worker.terminate()
        worker.join()

        # Close out dispatcher
        dispatcher.stop()
