import asyncio
import logging
import re
import time
from inspect import signature
from typing import Any, Callable, Generic, TypeVar, get_type_hints

import psycopg2
import requests
from pydantic import BaseModel, ValidationError
from sqlalchemy import Engine, create_engine
from sqlmodel import Session, select
from uuid_extensions import uuid7

from hyrex import constants
from hyrex.models import HyrexTask, StatusEnum

T = TypeVar("T", bound=BaseModel)


class UnboundTaskException(Exception):
    """Exception raised for errors in the task binding."""

    def __init__(self, message="Task is unbound."):
        self.message = message
        super().__init__(self.message)


class TaskRun:
    TASK_STATUS_PATH = "/connect/get-task-status"

    def __init__(
        self,
        task_name: str,
        task_run_id: str,
        status: StatusEnum,
        api_key: str = None,
        api_base_url: str = None,
        engine: Engine = None,
    ):
        self.task_name = task_name
        self.task_run_id = task_run_id
        self.status = status
        self.api_key = api_key
        self.api_base_url = api_base_url
        self.engine = engine

    def _update_status(self):
        if self.api_key:
            # Get task status using API
            status_url = f"{self.api_base_url}{self.TASK_STATUS_PATH}"
            headers = {
                "x-project-api-key": self.api_key,
            }
            data = {"task_ids": [str(self.task_run_id)]}
            try:
                response = requests.get(status_url, headers=headers, json=data)
                if response.status_code == 200:
                    response_data = response.json()
                    if response_data.get("result"):
                        task_instance = response_data.get("result")[0]
                    else:
                        raise Exception(
                            "Awaiting a task instance but task ID not returned from Hyrex platform."
                        )
                    self.status = task_instance.get("status")
                    return
                else:
                    logging.error(f"Error enqueuing task: {response.status_code}")
                    logging.error(f"Response body: {response.text}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Error enqueuing task via API: {str(e)}")
                raise RuntimeError(f"Failed to enqueue task via API: {e}")
        else:
            with Session(self.engine) as session:
                task_instance = session.get(HyrexTask, self.task_run_id)
                if task_instance is None:
                    raise Exception(
                        "Awaiting a task instance but task id not found in DB."
                    )

                self.status = task_instance.status

    def wait(self, timeout: float = 30.0, interval: float = 1.0):
        start = time.time()
        elapsed = 0
        while self.status in [StatusEnum.queued, StatusEnum.running]:
            if elapsed > timeout:
                raise TimeoutError("Waiting for task timed out.")
            self._update_status()
            time.sleep(interval)
            elapsed = time.time() - start

    def __repr__(self):
        return f"TaskRun<{self.task_name}>[{self.task_run_id}]"


class TaskWrapper(Generic[T]):
    ENQUEUE_TASK_PATH = "/connect/enqueue-task"

    def __init__(
        self,
        task_identifier: str,
        func: Callable[[T], Any],
        cron: str | None,
        queue: str = constants.DEFAULT_QUEUE,
        max_retries: int = 0,
        priority: int = constants.DEFAULT_PRIORITY,
    ):
        self.task_identifier = task_identifier
        self.func = func
        self.queue = queue
        self.signature = signature(func)
        self.type_hints = get_type_hints(func)
        self.cron = cron
        self.max_retries = max_retries
        self.priority = priority
        self.conn = None
        self.engine = None
        self.api_key = None
        self.api_base_url = None

        try:
            context_klass = next(iter(self.type_hints.values()))
        except StopIteration:
            raise ValidationError(
                "Hyrex expects all tasks to have 1 arg and for that arg to have a type hint."
            )

        self.context_klass = context_klass

    def set_api_key(self, api_key: str):
        self.api_key = api_key

    def set_api_base_url(self, api_base_url: str):
        self.api_base_url = api_base_url

    def set_conn(self, conn: str):
        if not self.conn and not self.engine:
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
        if self.api_key:
            raise NotImplementedError(
                "Task crons are not yet supported by the Hyrex platform."
            )

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
                    $$INSERT INTO public.hyrextask(id, root_id, task_name, status, queue, scheduled_start, started, finished, max_retries, args) VALUES(gen_random_uuid(), '{self.task_identifier}', 'queued'::statusenum, '{self.queue}', null, null, null, 0, '{{}}');$$
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

    def send(
        self,
        context: T,
        queue: str = None,
        priority: int = None,
        max_retries: int = None,
    ) -> TaskRun:
        logging.info(f"Sending task {self.func.__name__} to queue: {self.queue}")
        self._check_type(context)

        task = self._enqueue(context, queue, priority, max_retries)
        logging.info(f"Task sent off to queue: {context}")
        if self.api_key:
            return TaskRun(
                api_key=self.api_key,
                api_base_url=self.api_base_url,
                task_name=self.task_identifier,
                task_run_id=task.id,
                status=task.status,
            )
        else:
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
                else expected_type.model_validate(context)
            )
        except ValidationError as e:
            raise TypeError(
                f"Invalid argument type. Expected {expected_type.__name__}. Error: {e}"
            )

    def _enqueue(
        self,
        context: T,
        queue: str = None,
        priority: int = None,
        max_retries: int = None,
    ):
        task_id = uuid7()
        task_instance = HyrexTask(
            id=task_id,
            root_id=task_id,
            task_name=self.task_identifier,
            queue=queue or self.queue,
            args=context.model_dump(),
            max_retries=max_retries if max_retries is not None else self.max_retries,
            priority=priority if priority is not None else self.priority,
        )
        if self.api_key:
            # Enqueue task using API
            enqueue_url = f"{self.api_base_url}{self.ENQUEUE_TASK_PATH}"
            headers = {
                "x-project-api-key": self.api_key,
            }
            data = {
                "tasks": [
                    {
                        "id": str(task_instance.id),
                        "task_name": task_instance.task_name,
                        "queue": task_instance.queue,
                        "args": task_instance.args,
                        "max_retries": task_instance.max_retries,
                    }
                ]
            }
            try:
                response = requests.post(enqueue_url, headers=headers, json=data)
                if response.status_code == 200:
                    # Successfully enqueued, refresh the task instance
                    json_response = response.json()
                    task_instance = HyrexTask(**json_response.get("result")[0])
                    return task_instance
                else:
                    logging.error(f"Error enqueuing task: {response.status_code}")
                    logging.error(f"Response body: {response.text}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Error enqueuing task via API: {str(e)}")
                raise RuntimeError(f"Failed to enqueue task via API: {e}")
        else:
            # Enqueue task using database
            try:
                with Session(self._get_engine()) as session:
                    session.add(task_instance)
                    session.commit()
                    # Successfully enqueued, refresh the task instance
                    session.refresh(task_instance)
                    return task_instance
            except TypeError as e:
                raise RuntimeError(
                    "Task does not have a connection. If it's in a secondary register, make sure it's added to the main Hyrex instance."
                )

    def __repr__(self):
        return f"TaskWrapper<{self.task_identifier}>"
