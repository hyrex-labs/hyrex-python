import asyncio
import logging
import re
import time
from inspect import signature
from typing import Any, Callable, Generic, TypeVar, get_type_hints

import psycopg
from pydantic import BaseModel, ValidationError
from uuid_extensions import uuid7

from hyrex.dispatcher import Dispatcher
from hyrex.hyrex_context import get_hyrex_context
from hyrex.schemas import EnqueueTaskRequest, TaskStatus
from hyrex.task_config import TaskConfig
from hyrex.workflow.workflow_builder_context import get_current_workflow_builder

T = TypeVar("T", bound=BaseModel)


class UnboundTaskException(Exception):
    """Exception raised for errors in the task binding."""

    def __init__(self, message="Task is unbound."):
        self.message = message
        super().__init__(self.message)


class TaskRun:
    def __init__(
        self,
        task_name: str,
        task_run_id: str,
        dispatcher: Dispatcher,
    ):
        self.logger = logging.getLogger(__name__)

        self.task_name = task_name
        self.task_run_id = task_run_id
        self.dispatcher = dispatcher

    def wait(self, timeout: float = 30.0, interval: float = 1.0):
        start = time.time()
        elapsed = 0
        try:
            task_status = self.dispatcher.get_task_status(task_id=self.task_run_id)
        except ValueError:
            # Task hasn't yet moved from self.local_queue to DB
            task_status = TaskStatus.queued

        while task_status in [TaskStatus.queued, TaskStatus.running]:
            if elapsed > timeout:
                raise TimeoutError("Waiting for task timed out.")
            time.sleep(interval)
            task_status = self.dispatcher.get_task_status(task_id=self.task_run_id)
            elapsed = time.time() - start

    def cancel(self):
        self.dispatcher.try_to_cancel_task(self.task_run_id)

    def __repr__(self):
        return f"TaskRun<{self.task_name}>[{self.task_run_id}]"


def validate_error_handler(handler: Callable) -> None:
    sig = signature(handler)
    params = sig.parameters

    if len(params) > 1:
        raise ValueError("Hyrex on_error handler must accept either 0 or 1 arguments")

    if len(params) == 1:
        # Get the first (and only) parameter
        param = next(iter(params.values()))
        print(param.annotation)
        # Check its type annotation
        if param.annotation == param.empty:
            raise ValueError("Hyrex on_error handler must have type annotated args")
        if not issubclass(param.annotation, Exception):
            raise ValueError(
                "Hyrex on_error handler argument must be of type Exception"
            )


class TaskWrapper(Generic[T]):
    def __init__(
        self,
        task_identifier: str,
        func: Callable[[T], Any],
        dispatcher: Dispatcher,
        cron: str | None,
        task_config: TaskConfig,
        on_error: Callable = None,
    ):
        self.logger = logging.getLogger(__name__)

        self.task_identifier = task_identifier
        self.func = func
        self.signature = signature(func)
        self.type_hints = get_type_hints(func)
        self.cron = cron

        self.task_config = task_config

        self.dispatcher = dispatcher
        self.on_error = on_error

        if self.on_error:
            validate_error_handler(self.on_error)

        try:
            context_klass = next(iter(self.type_hints.values()))
        except StopIteration:
            raise ValidationError(
                "Hyrex expects all tasks to have 1 arg and for that arg to have a type hint."
            )

        self.context_klass = context_klass

    async def async_call(self, context: T):
        self.logger.info(
            f"Executing task {self.func.__name__} on queue: {self.task_config.queue}"
        )
        self._check_type(context)
        if asyncio.iscoroutinefunction(self.func):
            return await self.func(context)
        else:
            return self.func(context)

    # TODO: Re-implement
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
        with psycopg.connect(postgres_db) as conn:
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
                self.logger.info(f"{self.task_identifier} successfully scheduled.")

    # TODO: Re-implement
    def _unschedule(self):
        postgres_db = "/".join(self._get_conn().split("/")[:-1]) + "/postgres"
        sql = f"select cron.unschedule('{self.task_identifier}-cron');"
        with psycopg.connect(postgres_db) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql)
                    self.logger.info(f"Successfully unscheduled {self.task_identifier}")
                except Exception as e:
                    self.logger.warning(f"Unschedule failed with exception {e}")

    def withConfig(
        self,
        queue: str = None,
        priority: int = None,
        max_retries: int = None,
        timeout_seconds: int = None,
        idempotency_key: str = None,
    ) -> "TaskWrapper[T]":
        new_task_config = TaskConfig(
            queue=queue,
            priority=priority,
            max_retries=max_retries,
            timeout_seconds=timeout_seconds,
            idempotency_key=idempotency_key,
        )
        new_wrapper = TaskWrapper(
            task_identifier=self.task_identifier,
            func=self.func,
            dispatcher=self.dispatcher,
            cron=self.cron,
            task_config=self.task_config.merge(new_task_config),
        )
        return new_wrapper

    def get_queue(self):
        return self.task_config.queue

    def send(
        self,
        context: T,
    ) -> TaskRun:
        self.logger.info(
            f"Sending task {self.func.__name__} to queue: {self.task_config.queue}"
        )
        self._check_type(context)

        current_context = get_hyrex_context()

        task_id = uuid7()
        task = EnqueueTaskRequest(
            id=task_id,
            durable_id=task_id,
            root_id=current_context.root_id if current_context else task_id,
            parent_id=current_context.task_id if current_context else None,
            task_name=self.task_identifier,
            queue=self.task_config.get_queue_name(),
            args=context.model_dump(),
            max_retries=self.task_config.max_retries,
            timeout_seconds=self.task_config.timeout_seconds,
            priority=self.task_config.priority,
            idempotency_key=self.task_config.idempotency_key,
            status=TaskStatus.queued,
            workflow_run_id=None,
            workflow_dependencies=None,
        )

        self.dispatcher.enqueue([task])

        return TaskRun(
            task_name=self.task_identifier,
            task_run_id=task.id,
            dispatcher=self.dispatcher,
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

    def __repr__(self):
        return f"TaskWrapper<{self.task_identifier}>"

    # Methods for workflows:
    def __rshift__(self, other):
        builder = get_current_workflow_builder()
        if builder is None:
            raise RuntimeError(
                "No current workflow builder found. Please ensure you are within a workflow context."
            )
        # The builder is expected to have get_or_create_node.
        node = builder.get_or_create_node(self)
        return node >> other

    def __rrshift__(self, other):
        # This method is invoked when a TaskWrapper is on the right of >> and the left operand
        # does not implement __rshift__. We check if 'other' is a list.
        if isinstance(other, list):
            # Convert each element in the list to a DagNode (if needed)
            builder = get_current_workflow_builder()
            if builder is None:
                raise RuntimeError(
                    "No current workflow builder found. Please use a workflow context or decorator."
                )
            nodes = []
            for item in other:
                if isinstance(item, TaskWrapper):
                    # Note to developers: This path may be impossible?
                    node = builder.get_or_create_node(item)
                elif hasattr(item, "workflow_builder"):  # already a DagNode
                    node = item
                else:
                    raise TypeError(
                        f"Cannot use item of type {type(item)} as a task in the workflow."
                    )
                nodes.append(node)
            # Now, chain all the nodes in the list with self.
            # For example, we add self as a child to each node.
            for node in nodes:
                node >> self
            return self
        raise TypeError(f"Unsupported left operand type for >>: {type(other)}")
