import functools
import logging
import os
from typing import Any, Callable

from hyrex import constants
from hyrex.config import EnvVars
from hyrex.dispatcher import Dispatcher, get_dispatcher
from hyrex.task import T, TaskWrapper


class HyrexRegistry(dict[str, TaskWrapper]):
    def __setitem__(self, key: str, value: TaskWrapper):
        if not isinstance(key, str):
            raise TypeError("Key must be an instance of str")
        if not isinstance(value, TaskWrapper):
            raise TypeError("Value must be an instance of TaskWrapper")
        if key in self.keys():
            raise KeyError(
                f"Task {key} is already registered. Task names must be unique."
            )

        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> TaskWrapper:
        if not isinstance(key, str):
            raise TypeError("Key must be an instance of str")
        return super().__getitem__(key)

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if os.getenv(EnvVars.WORKER_PROCESS):
            self.dispatcher = None
        else:
            self.dispatcher = get_dispatcher()

    def set_dispatcher(self, dispatcher: Dispatcher):
        self.dispatcher = dispatcher
        for task_wrapper in self.values():
            task_wrapper.dispatcher = dispatcher

    def task(
        self,
        func=None,
        *,
        queue=constants.DEFAULT_QUEUE,
        cron=None,
        max_retries=0,
        priority=constants.DEFAULT_PRIORITY,
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
            )
            self[task_identifier] = task_wrapper

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
