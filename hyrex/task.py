import asyncio
import logging
import re
import time
from inspect import signature
from typing import Any, Callable, Generic, TypeVar, get_type_hints

import psycopg2
from pydantic import BaseModel, ValidationError
from sqlalchemy import Engine, create_engine
from sqlmodel import Session, select

from hyrex.models import HyrexTask, HyrexTaskResult, StatusEnum

T = TypeVar("T", bound=BaseModel)


class UnboundTaskException(Exception):
    """Exception raised for errors in the task binding."""

    def __init__(self, message="Task is unbound."):
        self.message = message
        super().__init__(self.message)


class TaskRun:
    def __init__(
        self, engine: Engine, task_name: str, task_run_id: str, status: StatusEnum
    ):
        self.task_name = task_name
        self.task_run_id = task_run_id
        self.engine = engine
        self.status = status

    def wait(self, timeout=30, interval=1):
        start = time.time()
        elapsed = 0
        while self.status in [StatusEnum.queued, StatusEnum.running]:
            if elapsed > timeout:
                raise TimeoutError("Waiting for task timed out.")
            with Session(self.engine) as session:
                task_instance = session.get(HyrexTask, self.task_run_id)
                if task_instance is None:
                    raise Exception(
                        "Awaiting a task instance but task id not found in DB."
                    )

                self.status = task_instance.status
            time.sleep(interval)
            elapsed = time.time() - start

    def get_result(self) -> dict:
        with Session(self.engine) as session:
            statement = (
                select(HyrexTaskResult)
                .join(HyrexTask)
                .where(HyrexTask.id == self.task_run_id)
            )
            task_result = session.exec(statement).first()
            if not task_result:
                raise Exception(f"Result not found for task id: {self.task_run_id}")

            return task_result.results

    def __repr__(self):
        return f"TaskRun<{self.task_name}>[{self.task_run_id}]"


class TaskWrapper(Generic[T]):
    def __init__(
        self,
        task_identifier: str,
        func: Callable[[T], Any],
        queue: str,
        cron: str | None,
    ):
        self.task_identifier = task_identifier
        self.func = func
        self.queue = queue
        self.signature = signature(func)
        self.type_hints = get_type_hints(func)
        self.cron = cron
        self.conn = None
        self.engine = None

        try:
            context_klass = next(iter(self.type_hints.values()))
        except StopIteration:
            raise ValidationError(
                "Hyrex expects all tasks to have 1 arg and for that arg to have a type hint."
            )

        self.context_klass = context_klass
        self._task_instance_id = None

    def set_conn(self, conn):
        self.conn = conn
        self.engine = create_engine(conn)

    def _get_conn(self):
        if self.conn is None:
            raise RuntimeError(
                f"Task {self.task_identifier} has no associated connection. Has it been registered with the main Hyrex app instance?"
            )
        return self.conn

    def _get_engine(self):
        if self.engine is None:
            raise RuntimeError(
                f"Task {self.task_identifier} has no associated connection. Has it been registered with the main Hyrex app instance?"
            )
        return self.engine

    async def async_call(self, context: T):
        logging.info(f"Executing task {self.func.__name__} on queue: {self.queue}")
        self._check_type(context)
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(context)
        else:
            return self.func(context)

    def __call__(self, context: T):
        self._check_type(context)
        return self.func(context)

    def schedule(self):
        if not self.cron:
            self._unschedule()
            return

        cron_regex = r"(@(annually|yearly|monthly|weekly|daily|hourly|reboot))|(@every (\d+(ns|us|µs|ms|s|m|h))+)|((((\d+,)+\d+|([\d\*]+(\/|-)\d+)|\d+|\*) ?){5,7})"
        is_valid = bool(re.fullmatch(cron_regex, self.cron))
        if not is_valid:
            raise ValidationError(f"Cron Expression is not valid: {self.cron}")

        target_db_name = self._get_conn().split("/")[-1]
        postgres_db = "/".join(self._get_conn().split("/")[:-1]) + "/postgres"
        with psycopg2.connect(postgres_db) as conn:
            with conn.cursor() as cur:
                sql = f"""
                select
                cron.schedule(
                    '{self.task_identifier}-cron',
                    '{self.cron}',
                    $$INSERT INTO public.hyrextask(id, task_name, status, queue, scheduled_start, started, finished, retried, args) VALUES(gen_random_uuid(), '{self.task_identifier}', 'queued'::statusenum, '{self.queue}', null, null, null, 0, '{{}}');$$
                    );

                UPDATE cron.job SET database = '{target_db_name}' WHERE jobname = '{self.task_identifier}-cron';
                """
                result = cur.execute(sql)
                conn.commit()
                print(f"{self.task_identifier} successfully scheduled.")

    def _unschedule(self):
        postgres_db = "/".join(self._get_conn().split("/")[:-1]) + "/postgres"
        sql = f"select cron.unschedule('{self.task_identifier}-cron');"
        with psycopg2.connect(postgres_db) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    print(f"Successfully unscheduled {self.task_identifier}")
                except Exception as e:
                    pass
                    # print(f"Unschedule failed with exception {e}")

    def send(self, context: T) -> TaskRun:
        logging.info(f"Sending task {self.func.__name__} to queue: {self.queue}")
        self._check_type(context)

        task = self._enqueue(context)
        logging.info(f"Task sent off to queue: {context}")
        return TaskRun(
            engine=self._get_engine(),
            task_name=self.task_identifier,
            task_run_id=task.id,
            status=task.status,
        )

    def _check_type(self, context: T):
        expected_type = next(iter(self.type_hints.values()))
        try:
            validated_arg = (
                expected_type.parse_obj(context)
                if isinstance(context, dict)
                else expected_type.validate(context)
            )
        except ValidationError as e:
            raise TypeError(
                f"Invalid argument type. Expected {expected_type.__name__}. Error: {e}"
            )

    def _enqueue(self, context: T):
        try:
            with Session(self._get_engine()) as session:
                task_instance = HyrexTask(
                    task_name=self.task_identifier,
                    queue=self.queue,
                    args=context.model_dump(),
                )
                session.add(task_instance)
                session.commit()
                session.refresh(task_instance)
                self._task_instance_id = task_instance.id
                return task_instance
        except TypeError as e:
            raise RuntimeError(
                "Task does not have a connection. If it's in a secondary register, make sure the "
            )

    def __repr__(self):
        return f"TaskWrapper<{self.task_identifier}>"
