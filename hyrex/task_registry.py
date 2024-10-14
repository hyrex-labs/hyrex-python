# from hyrex.decorator import TaskDecoratorProvider
import functools
from typing import Any, Callable

from hyrex import constants
from hyrex.task import T, TaskWrapper


class TaskRegistry(dict[str, "TaskWrapper"]):
    def __setitem__(self, key: str, value: "TaskWrapper"):
        if not isinstance(key, str):
            raise TypeError("Key must be an instance of str")
        if not isinstance(value, TaskWrapper):
            raise TypeError("Value must be an instance of TaskWrapper")
        if key in self.keys():
            raise KeyError(
                f"Task {key} is already registered. Task names must be unique."
            )

        super().__setitem__(key, value)

    def __getitem__(self, key: str) -> "TaskWrapper":
        if not isinstance(key, str):
            raise TypeError("Key must be an instance of str")
        return super().__getitem__(key)

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
            )
            self[task_identifier] = task_wrapper

            @functools.wraps(func)
            def wrapper(context: T) -> Any:
                return task_wrapper(context)

            wrapper.send = task_wrapper.send
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def add_registry(self, task_registry: "TaskRegistry"):
        for key, val in task_registry.items():
            self[key] = val

    def set_connection(self, conn: str, api_key: str, api_base_url: str):
        for task_wrapper in self.values():
            if conn:
                task_wrapper.set_conn(conn)
            if api_key:
                task_wrapper.set_api_key(api_key)
            if api_base_url:
                task_wrapper.set_api_base_url(api_base_url)

    def schedule(self):
        for task_wrapper in self.values():
            task_wrapper.schedule()
