import asyncio
import logging
import multiprocessing

import psycopg2  # Added import for database connections
import pytest
from pydantic import BaseModel

from hyrex.app import Hyrex
from hyrex.models import create_tables

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


class EmptyContext(BaseModel):
    pass


class NumberContext(BaseModel):
    number: int


def register_tasks(hy: Hyrex):
    @hy.task
    def empty_task(context: EmptyContext):
        print("Completed empty task.")

    @hy.task
    def error_task(context: EmptyContext):
        raise RuntimeError("This task has raised an error.")

    @hy.task
    def number_task(context: NumberContext):
        print(f"Received number {context.number}")

    return empty_task, error_task, number_task


def worker_process(db_connection_string):
    hy = Hyrex(app_id="hyrex-smoke-test", conn=db_connection_string)
    register_tasks(hy)
    hy.run_worker()


@pytest.fixture
def db_connection_string(postgresql):
    db_info = postgresql.info

    connection_string = (
        f"postgresql://{db_info.user}:{db_info.password}"
        f"@{db_info.host}:{db_info.port}/{db_info.dbname}"
    )
    return connection_string


def clear_db(db_connection_string: str):
    conn = psycopg2.connect(db_connection_string)
    try:
        with conn.cursor() as cursor:
            cursor.execute("DELETE FROM hyrextask;")
    finally:
        conn.close()


def get_completed_tasks(db_connection_string: str):
    conn = psycopg2.connect(db_connection_string)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM hyrextask WHERE status='success';")
            result = cursor.fetchone()
            return result[0] if result else 0
    finally:
        conn.close()


def get_failed_tasks(db_connection_string: str):
    conn = psycopg2.connect(db_connection_string)
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

    hy = Hyrex(app_id="hyrex-smoke-test", conn=db_connection_string)
    empty_task, error_task, number_task = register_tasks(hy)

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
        error_task.send(EmptyContext(), max_retries=3)
        await asyncio.sleep(5)
        assert get_failed_tasks(db_connection_string) == 4
        clear_db(db_connection_string)

        # Many tasks queued/run
        for i in range(1, 20):
            number_task.send(NumberContext(number=i))
        await asyncio.sleep(3)
        assert get_completed_tasks(db_connection_string) == 20
        clear_db(db_connection_string)

    finally:
        worker.terminate()
        worker.join()
