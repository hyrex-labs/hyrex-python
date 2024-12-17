import functools
import logging
import os
from inspect import signature
from typing import Any, Callable

from hyrex import constants
from hyrex.config import EnvVars
from hyrex.dispatcher import Dispatcher, get_dispatcher
from hyrex.hyrex_queue import HyrexQueue
from hyrex.task import T, TaskWrapper


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
        self.logger.debug(f"All tasks: {self.internal_task_registry.keys()}")
        if isinstance(task_wrapper.queue, str):
            self.register_queue(HyrexQueue(name=task_wrapper.queue))
        else:
            self.register_queue(task_wrapper.queue)

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

    def get_registered_tasks(self):
        return self.internal_task_registry.values()

    def get_task(self, task_name: str):
        return self.internal_task_registry[task_name]

    def add_registry(self, registry: "HyrexRegistry"):
        for task_wrapper in registry.get_registered_tasks():
            self.register_task(task_wrapper=task_wrapper)

    def task(
        self,
        func: Callable = None,
        *,
        queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
        cron: str = None,
        max_retries: int = 0,
        priority: int = constants.DEFAULT_PRIORITY,
        on_error: Callable = None,
    ) -> TaskWrapper:
        """
        Create task decorator
        """

        def decorator(func: Callable[[T], Any]) -> Callable[[T], Any]:
            task_identifier = func.__name__
            task_wrapper = TaskWrapper(
                task_identifier=task_identifier,
                func=func,
                queue=queue,
                cron=cron,
                max_retries=max_retries,
                priority=priority,
                dispatcher=self.dispatcher,
                on_error=on_error,
            )
            self.register_task(task_wrapper=task_wrapper)

            @functools.wraps(func)
            def wrapper(context: T) -> Any:
                return task_wrapper(context)

            wrapper.send = task_wrapper.send
            wrapper.withConfig = task_wrapper.withConfig
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def schedule(self):
        for task_wrapper in self.values():
            task_wrapper.schedule()
