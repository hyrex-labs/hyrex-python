import functools
from hyrex.task import T, TaskWrapper
from typing import Any, Callable, Generic

from hyrex import constants
from hyrex.dispatcher import Dispatcher
from hyrex.hyrex_queue import HyrexQueue
from typing import Sequence, Self, Callable
import collections


class HyrexWorkflow:
    def __init__(self, workflow_identifier, func, workflow_args_schema=None):
        self.root_nodes = []
        self.workflow_identifier = workflow_identifier
        self.workflow_args_schema = None

    def __rshift__(self, other: Self | Sequence[Self]) -> Self:
        if isinstance(other, DagNode):
            self.root_nodes.append(other)
        elif isinstance(other, collections.abc.Sequence):
            for child in other:
                self.root_nodes.append(child)
        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

        return other  # Allows chaining like task1 >> task2 >> task3

    def send(self, *args, **kwargs):
        print(f"Sending... {self.workflow_identifier}")

    def to_json(self):
        pass

    def from_json(self):
        pass

class DagNode:
    def __init__(self):
        pass

    def add_child(self):
        pass


class TaskWrapper(Generic[T], DagNode):
    def __init__(
            self,
            task_identifier: str,
            func: Callable[[T], Any],
            dispatcher: Dispatcher,
            cron: str | None,
            queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
            max_retries: int = 0,
            timeout_seconds: int = 0,
            priority: int = constants.DEFAULT_PRIORITY,
            idempotency_key: str = None,
            on_error: Callable = None,
    ):
        self.task_identifier = task_identifier
        self.func = func

    def send(self, *args, **kwargs):
        pass

    def __rshift__(self, other: Self | Sequence[Self]) -> Self:
        if isinstance(other, DagNode):
            self.add_child(other)
        elif isinstance(other, collections.abc.Sequence):
            for child in other:
                self.add_child(child)
        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

        return other  # Allows chaining like task1 >> task2 >> task3

    def __rrshift__(self, other: Sequence[Self]):
        if not isinstance(other, collections.abc.Sequence):
            raise TypeError(f"Unknown type of {other}, {type(other)}")
        for parent in other:
            if not isinstance(parent, DagNode):
                raise TypeError(f"Cannot use object {parent} of type {type(parent)} as node in DAG.")
            parent.add_child(self)

        return other  # Allows chaining like task1 >> task2 >> task3


class HyrexRegistry:
    def task(
            self,
            func: Callable = None
    ) -> TaskWrapper:
        """
        Create task decorator
        """

        def decorator(func: Callable[[T], Any]) -> Callable[[T], Any]:
            task_identifier = func.__name__
            task_wrapper = TaskWrapper(
                task_identifier=task_identifier,
                func=func,
                # queue=queue,
                # cron=cron,
                # max_retries=max_retries,
                # timeout_seconds=timeout_seconds,
                # priority=priority,
                # dispatcher=self.dispatcher,
                # on_error=on_error,
            )

            @functools.wraps(func)
            def wrapper(context: T) -> Any:
                return task_wrapper(context)

            wrapper.send = task_wrapper.send
            wrapper.withConfig = task_wrapper.withConfig
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def workflow(self, func: Callable = None):

        def decorator(func: Callable[[T], Any]):
            task_identifier = func.__name__
            hyrex_workflow = HyrexWorkflow(
                workflow_identifier=task_identifier,
                func=func,
                # queue=queue,
                # cron=cron,
                # max_retries=max_retries,
                # timeout_seconds=timeout_seconds,
                # priority=priority,
                # dispatcher=self.dispatcher,
                # on_error=on_error,
            )
            return hyrex_workflow

        return decorator(func=func)

