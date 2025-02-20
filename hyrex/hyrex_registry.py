import functools
import inspect
import logging
import os
from inspect import signature
from typing import Any, Callable

from hyrex import constants
from hyrex.config import EnvVars
from hyrex.dispatcher import Dispatcher, get_dispatcher
from hyrex.hyrex_queue import HyrexQueue
from hyrex.task import T, TaskWrapper
from hyrex.task_config import TaskConfig
from hyrex.workflow.workflow import HyrexWorkflow
from hyrex.workflow.workflow_builder import WorkflowBuilder

# TODO: Also register tasks with DB here.


class HyrexRegistry:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if os.getenv(EnvVars.WORKER_PROCESS):
            self.dispatcher = None
        else:
            self.dispatcher = get_dispatcher()

        self.internal_task_registry: dict[str, TaskWrapper] = {}
        self.internal_queue_registry: dict[str, HyrexQueue] = {}

    def register_task(self, task_wrapper: TaskWrapper):
        self.logger.debug(f"Registering task: {task_wrapper.task_identifier}")
        if self.internal_task_registry.get(task_wrapper.task_identifier):
            raise KeyError(
                f"Task {task_wrapper.task_identifier} is already registered. Task names must be unique."
            )
        self.internal_task_registry[task_wrapper.task_identifier] = task_wrapper

        # Register the task wrapper's queue for tracking concurrency.
        queue = task_wrapper.get_queue()
        if isinstance(queue, str):
            self.register_queue(HyrexQueue(name=queue))
        else:
            self.register_queue(queue)

    def register_queue(self, queue: HyrexQueue):
        if self.internal_queue_registry.get(queue.name) and not queue.equals(
            self.internal_queue_registry[queue.name]
        ):
            raise KeyError(
                f"Conflicting concurrency limits on queue name: {queue.name}"
            )

        self.internal_queue_registry[queue.name] = queue

    def get_concurrency_limit(self, queue_name: str):
        # TODO: Add queue patterns?
        if self.internal_queue_registry.get(queue_name):
            return self.internal_queue_registry[queue_name].concurrency_limit
        else:
            return 0

    def set_dispatcher(self, dispatcher: Dispatcher):
        self.dispatcher = dispatcher
        for task_wrapper in self.internal_task_registry.values():
            task_wrapper.dispatcher = dispatcher

    def get_on_error_handler(self, task_name: str) -> Callable | None:
        task_wrapper = self.internal_task_registry[task_name]
        return task_wrapper.on_error

    def get_task_wrappers(self):
        return self.internal_task_registry.values()

    def get_task(self, task_name: str):
        return self.internal_task_registry[task_name]

    def add_registry(self, registry: "HyrexRegistry"):
        for task_wrapper in registry.get_task_wrappers():
            self.register_task(task_wrapper=task_wrapper)

    def task(
        self,
        func: Callable = None,
        *,
        queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
        cron: str = None,
        max_retries: int = 0,
        timeout_seconds: int | None = None,
        priority: int = constants.DEFAULT_PRIORITY,
        on_error: Callable = None,
    ) -> TaskWrapper:
        """
        Create task decorator
        """

        def decorator(func: Callable[[T], Any]) -> TaskWrapper:
            task_identifier = func.__name__
            task_config = TaskConfig(
                queue=queue,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
                priority=priority,
            )
            task_wrapper = TaskWrapper(
                task_identifier=task_identifier,
                func=func,
                cron=cron,
                task_config=task_config,
                dispatcher=self.dispatcher,
                on_error=on_error,
            )
            self.register_task(task_wrapper=task_wrapper)
            return task_wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def schedule(self):
        for task_wrapper in self.values():
            task_wrapper.schedule()

    def workflow(
        self,
        name: str,
        queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
        max_retries: int = 0,
        timeout_seconds: int | None = None,
        priority: int = constants.DEFAULT_PRIORITY,
        cron: str = None,
        workflow_arg_schema=None,
    ):
        """
        A decorator to register a workflow.
        """

        def decorator(func):
            task_config = TaskConfig(
                queue=queue,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
                priority=priority,
            )

            with WorkflowBuilder() as workflow_builder:
                # Build the workflow by calling the function.
                func()

                # Register workflow on publisher (on worker processes, self.dispatcher won't be set yet)
                if self.dispatcher:
                    source_code = inspect.getsource(func)
                    self.dispatcher.register_workflow(
                        name=name,
                        source_code=source_code,
                        workflow_dag_json=workflow_builder.to_json(),
                    )

                # Create and return a HyrexWorkflow instance
                workflow = HyrexWorkflow(
                    name=name,
                    task_config=task_config,
                    workflow_arg_schema=workflow_arg_schema,
                    workflow_builder=workflow_builder,
                    dispatcher=self.dispatcher,
                )

            return workflow

        return decorator
