import asyncio
import logging
import re
import time
from inspect import signature
from typing import Any, Callable, Generic, ParamSpec, TypeVar, get_type_hints, overload

import psycopg
from pydantic import BaseModel, ValidationError
from uuid6 import uuid7

from hyrex.configs import ConfigPhase, TaskConfig
from hyrex.dispatcher import Dispatcher
from hyrex.durable_run import DurableTaskRun
from hyrex.hyrex_context import get_hyrex_context
from hyrex.hyrex_queue import HyrexQueue
from hyrex.schemas import EnqueueTaskRequest, TaskStatus
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


P = ParamSpec("P")  # Captures the parameter specification of the wrapped function
R = TypeVar("R")  # Captures the return type of the wrapped function


class TaskWrapper(Generic[P, R]):
    class ParamInfo(BaseModel):
        """Pydantic model to store parameter information"""

        type_hint: Any
        default: Any = None
        has_default: bool = False

        def __repr__(self):
            type_name = (
                self.type_hint.__name__
                if hasattr(self.type_hint, "__name__")
                else str(self.type_hint)
            )
            return f"ParamInfo(type_hint={type_name}, default={self.default}, has_default={self.has_default})"

        class Config:
            arbitrary_types_allowed = True  # Allow any Python type in type_hint field

    def __init__(
        self,
        task_identifier: str,
        func: Callable[P, R],
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

        # Track the parameter names and their type hints for validation
        # Enforce that all parameters have type hints
        self.param_info = {}
        for param_name, param in self.signature.parameters.items():
            # Check if type hint exists for this parameter
            if param_name not in self.type_hints:
                raise TypeError(
                    f"Hyrex expects all task arguments to have type hints. Argument '{param_name}' in task '{task_identifier}' has no type hint."
                )

            # Create a Pydantic model instance for this parameter
            self.param_info[param_name] = self.ParamInfo(
                type_hint=self.type_hints.get(param_name),
                default=param.default if param.default is not param.empty else None,
                has_default=param.default is not param.empty,
            )

    async def async_call(self, **kwargs):
        self.logger.info(f"Executing task {self.func.__name__}.")

        # Validate kwargs against expected parameters
        validated_kwargs = self._validate_kwargs(kwargs)

        if asyncio.iscoroutinefunction(self.func):
            return await self.func(**validated_kwargs)
        else:
            return self.func(**validated_kwargs)

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
            retry_backoff=self.retry_backoff,
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

    def send(self, *args: P.args, **kwargs: P.kwargs) -> DurableTaskRun:
        """
        Send this task to the Hyrex queue with the provided parameters.

        This method accepts the same parameters as the wrapped function.
        """
        self.logger.debug(
            f"Sending task {self.func.__name__} to queue: {self.task_config.queue}"
        )

        # Convert positional args to kwargs based on function signature
        bound_args = self.signature.bind_partial(*args, **kwargs)
        bound_args.apply_defaults()
        
        # Validate the combined args against our function signature
        validated_kwargs = self._validate_kwargs(bound_args.arguments)

        current_context = get_hyrex_context()

        task_id = uuid7()
        task = EnqueueTaskRequest(
            id=task_id,
            durable_id=task_id,
            root_id=current_context.root_id if current_context else task_id,
            parent_id=current_context.task_id if current_context else None,
            task_name=self.task_identifier,
            queue=self.task_config.get_queue_name(),
            args=validated_kwargs,  # Pass the validated kwargs directly
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

    def _validate_kwargs(self, kwargs):
        """
        Validate that the provided kwargs match the function signature.
        Apply type coercion if possible, otherwise raise appropriate errors.
        """
        validated_kwargs = {}

        # Check for missing required arguments
        for param_name, info in self.param_info.items():
            if param_name not in kwargs and not info.has_default:
                raise TypeError(
                    f"Missing required argument '{param_name}' for task '{self.task_identifier}'"
                )

        # Process provided arguments
        for param_name, value in kwargs.items():
            if param_name not in self.param_info:
                raise TypeError(
                    f"Unexpected argument '{param_name}' for task '{self.task_identifier}'"
                )

            param_type = self.param_info[param_name].type_hint

            # If we have a type hint, try to validate/convert the value
            if param_type is not None:
                try:
                    # Handle Pydantic models for backward compatibility
                    if hasattr(param_type, "model_validate"):
                        validated_kwargs[param_name] = param_type.model_validate(value)
                    elif hasattr(param_type, "parse_obj"):
                        validated_kwargs[param_name] = param_type.parse_obj(value)
                    # For primitive types, try basic conversion
                    elif param_type in (int, float, str, bool) and not isinstance(
                        value, param_type
                    ):
                        try:
                            validated_kwargs[param_name] = param_type(value)
                        except (ValueError, TypeError):
                            raise TypeError(
                                f"Cannot convert argument '{param_name}' value '{value}' to expected type {param_type.__name__}"
                            )
                    else:
                        # For other types, just pass the value as is
                        validated_kwargs[param_name] = value
                except Exception as e:
                    raise TypeError(
                        f"Validation error for argument '{param_name}': {str(e)}"
                    )
            else:
                # No type hint, just use the value as is
                validated_kwargs[param_name] = value

        # Add default values for missing arguments
        for param_name, info in self.param_info.items():
            if param_name not in kwargs and info.has_default:
                validated_kwargs[param_name] = info.default

        return validated_kwargs

    def get_arg_schema(self):
        """
        Return a schema describing the expected arguments for this task.
        This is useful for documentation and UI generation.
        """
        schema = {}
        for param_name, info in self.param_info.items():
            param_type = info.type_hint
            param_schema = {
                "required": not info.has_default,
                "type": param_type.__name__ if param_type else "any",
            }

            if info.has_default and info.default is not None:
                param_schema["default"] = str(info.default)

            # For Pydantic models, include their schema if available
            if param_type and hasattr(param_type, "model_json_schema"):
                param_schema["schema"] = param_type.model_json_schema()

            schema[param_name] = param_schema

        return schema

    def __repr__(self):
        return f"TaskWrapper<{self.task_identifier}>"

    def __call__(self, *args: P.args, **kwargs: P.kwargs):
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
