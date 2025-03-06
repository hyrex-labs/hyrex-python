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
from hyrex.durable_run import DurableTaskRun
from hyrex.hyrex_context import get_hyrex_context
from hyrex.hyrex_queue import HyrexQueue
from hyrex.schemas import EnqueueTaskRequest, TaskStatus
from hyrex.configs import ConfigPhase, TaskConfig
from hyrex.workflow.workflow_builder_context import get_current_workflow_builder


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


class TaskWrapper:
    def __init__(
        self,
        task_identifier: str,
        func: Callable,
        dispatcher: Dispatcher,
        cron: str | None,
        task_config: TaskConfig,
        on_error: Callable = None,
        retry_backoff: int | Callable[[int], int] | None = None,
    ):
        self.logger = logging.getLogger(__name__)

        self.task_identifier = task_identifier
        self.func = func
        self.signature = signature(func)
        self.type_hints = get_type_hints(func)

        # TODO: Validate cron
        self.cron = cron

        self.task_config = task_config

        self.dispatcher = dispatcher
        self.on_error = on_error
        self.retry_backoff = retry_backoff

        if self.on_error:
            validate_error_handler(self.on_error)

        # Check if function has arguments
        if self.signature.parameters:
            try:
                # Get the first parameter
                param_name = next(iter(self.signature.parameters))

                # Check if type hint exists
                if param_name not in self.type_hints:
                    raise TypeError(
                        f"Hyrex expects all task arguments to have type hints. Argument '{param_name}' has no type hint."
                    )

                self.context_klass = self.type_hints.get(param_name)

                # Check if it's a Pydantic model
                if self.context_klass is not None and not (
                    hasattr(self.context_klass, "model_validate")
                    or hasattr(self.context_klass, "parse_obj")
                ):
                    raise TypeError(
                        f"Hyrex expects task arguments to be Pydantic models. {self.context_klass.__name__} is not a valid Pydantic model."
                    )
            except StopIteration:
                self.context_klass = None
        else:
            self.context_klass = None

    def get_arg_schema(self):
        return self.context_klass

    async def async_call(self, context=None):
        self.logger.info(f"Executing task {self.func.__name__}.")

        if context is not None and self.context_klass is not None:
            self._check_type(context)
            if asyncio.iscoroutinefunction(self.func):
                return await self.func(context)
            else:
                return self.func(context)
        else:
            # No arguments
            if asyncio.iscoroutinefunction(self.func):
                return await self.func()
            else:
                return self.func()

    def with_config(
        self,
        queue: str | HyrexQueue = None,
        priority: int = None,
        max_retries: int = None,
        timeout_seconds: int = None,
        idempotency_key: str = None,
    ) -> "TaskWrapper":
        new_task_config = TaskConfig(
            config_phase=ConfigPhase.send,
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
            on_error=self.on_error,
        )
        return new_wrapper

    def get_queue(self) -> HyrexQueue | str:
        return self.task_config.queue

    def get_retry_backoff(self, attempt_number: int) -> int:
        if self.retry_backoff is None:
            return 0
        elif isinstance(self.retry_backoff, int):
            return self.retry_backoff
        elif callable(self.retry_backoff):
            return self.retry_backoff(attempt_number)
        else:
            raise RuntimeError(
                f"Unsupported type for retry_backoff in task {self.task_identifier}"
            )

    def send(
        self,
        context=None,
    ) -> DurableTaskRun:
        self.logger.debug(
            f"Sending task {self.func.__name__} to queue: {self.task_config.queue}"
        )

        # TODO: Improve this arg-checking logic
        # Only perform type checking if we expect a context
        if context is not None and self.context_klass is not None:
            self._check_type(context)
            args = context.model_dump() if hasattr(context, "model_dump") else {}
        elif context is not None and self.context_klass is None:
            # Task was defined with no arguments but context was provided
            raise TypeError(
                f"Task {self.task_identifier} was defined with no arguments, but arguments were provided."
            )
        else:
            args = {}

        current_context = get_hyrex_context()

        task_id = uuid7()
        task = EnqueueTaskRequest(
            id=task_id,
            durable_id=task_id,
            root_id=current_context.root_id if current_context else task_id,
            parent_id=current_context.task_id if current_context else None,
            task_name=self.task_identifier,
            queue=self.task_config.get_queue_name(),
            args=args,
            max_retries=self.task_config.max_retries,
            timeout_seconds=self.task_config.timeout_seconds,
            priority=self.task_config.priority,
            idempotency_key=self.task_config.idempotency_key,
            status=TaskStatus.queued,
            workflow_run_id=None,
            workflow_dependencies=None,
        )

        self.dispatcher.enqueue([task])

        return DurableTaskRun(
            task_name=self.task_identifier,
            durable_id=task.id,
            dispatcher=self.dispatcher,
        )

    def _check_type(self, context):
        if self.context_klass is None:
            return

        try:
            if isinstance(context, dict):
                validated_arg = self.context_klass.parse_obj(context)
            elif hasattr(self.context_klass, "model_validate"):
                validated_arg = self.context_klass.model_validate(context)
            else:
                # If not a Pydantic model, assume it's the correct type
                return
        except ValidationError as e:
            raise TypeError(
                f"Invalid argument type. Expected {self.context_klass.__name__}. Error: {e}"
            )

    def __repr__(self):
        return f"TaskWrapper<{self.task_identifier}>"

    def __call__(self, *args, **kwargs):
        # Simply pass through all arguments to the original function
        return self.func(*args, **kwargs)

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
