import functools
from hyrex.task import T, TaskWrapper
from typing import Any, Callable, Generic

from hyrex import constants
from hyrex.dispatcher import Dispatcher
from hyrex.hyrex_queue import HyrexQueue
from typing import Sequence, Self, Callable
import collections


class DagNode:
    def __init__(self, task_wrapper: TaskWrapper):
        self.task_wrapper = task_wrapper
        self.children = []

    def has_path_to(self, target: "DagNode", visited=None) -> bool:
        if visited is None:
            visited = set()
        if self is target:
            return True
        visited.add(self)
        for child in self.children:
            if child not in visited and child.has_path_to(target, visited):
                return True
        return False

    def add_child(self, child: "DagNode"):
        # Check if there's already a path from the new child back to self.
        # If so, adding child would create a cycle.
        if child.has_path_to(self):
            raise ValueError("Adding this child would create a cycle!")
        self.children.append(child)

    def send(self, *args, **kwargs):
        # TODO
        pass

    def __rshift__(
        self, other: "DagNode" | Sequence["DagNode"]
    ) -> "DagNode" | Sequence["DagNode"]:
        if isinstance(other, DagNode):
            self.add_child(other)
        elif isinstance(other, collections.abc.Sequence):
            for child in other:
                self.add_child(child)
        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

        return other  # Allows chaining like task1 >> task2 >> task3

    def __rrshift__(self, other: Sequence["DagNode"]) -> "DagNode":
        if not isinstance(other, collections.abc.Sequence):
            raise TypeError(f"Unknown type of {other}, {type(other)}")
        for parent in other:
            if not isinstance(parent, DagNode):
                raise TypeError(
                    f"Cannot use object {parent} of type {type(parent)} as node in DAG."
                )
            parent.add_child(self)

        return other  # Allows chaining like task1 >> task2 >> task3


class HyrexWorkflow:
    def __init__(self, workflow_identifier, func, workflow_args_schema=None):
        self.root_nodes = []
        self.workflow_identifier = workflow_identifier
        self.workflow_args_schema = None

    def __rshift__(
        self, other: TaskWrapper | Sequence[TaskWrapper]
    ) -> DagNode | list[DagNode]:
        if isinstance(other, TaskWrapper):
            new_node = DagNode(task_wrapper=other)
            self.root_nodes.append(new_node)
            return new_node

        elif isinstance(other, collections.abc.Sequence):
            new_nodes = [DagNode(child) for child in other]
            self.root_nodes += new_nodes
            return new_nodes

        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

    def send(self, *args, **kwargs):
        print(f"Sending... {self.workflow_identifier}")

    def to_json(self):
        pass

    def from_json(self):
        pass


class HyrexRegistry:
    def task(self, func: Callable = None) -> TaskWrapper:
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
