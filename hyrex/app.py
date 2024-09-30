import functools
import logging
import os
from typing import Any, Callable

from hyrex.async_worker import AsyncWorker, T, TaskRegistry, TaskWrapper


class EnvVars:
    DATABASE_URL = "HYREX_DATABASE_URL"
    API_KEY = "HYREX_API_KEY"


class Hyrex:
    def __init__(
        self,
        app_id: str,
        conn: str = os.getenv(EnvVars.DATABASE_URL),
        api_key: str = os.getenv(EnvVars.API_KEY),
        error_callback: Callable = None,
    ):
        self.app_id = app_id
        self.task_registry: TaskRegistry = TaskRegistry()
        self.conn = conn
        self.api_key = api_key
        self.error_callback = error_callback

    def task(self, func=None, *, queue="default", cron=None) -> TaskWrapper:
        """
        Create task decorator
        """
        if func is None:
            # Decorator called with parentheses
            def decorator(func: Callable[[T], Any]) -> Callable[[T], Any]:
                task_identifier = func.__name__
                task_wrapper = TaskWrapper(
                    task_identifier=task_identifier,
                    func=func,
                    queue=queue,
                    conn=self.conn,
                    cron=cron,
                )
                self.task_registry[task_identifier] = task_wrapper

                @functools.wraps(func)
                def wrapper(context: T) -> TaskWrapper:
                    return task_wrapper(context)

                wrapper.send = task_wrapper.send
                return wrapper

            return decorator
        else:
            # Decorator called without parentheses
            task_identifier = func.__name__
            task_wrapper = TaskWrapper(
                task_identifier, func, "default", self.conn, cron
            )
            self.task_registry[task_identifier] = task_wrapper

            @functools.wraps(func)
            def wrapper(context: T) -> Any:
                return task_wrapper(context)

            wrapper.send = task_wrapper.send
            return wrapper

    def schedule(self):
        for task in self.task_registry.values():
            task.schedule()

    def run_worker(
        self,
        queue: str = "default",
        num_threads: int = 8,
        log_level: int = logging.INFO,
    ):
        logging.basicConfig(level=log_level)

        worker = AsyncWorker(
            conn=self.conn,
            queue=queue,
            task_registry=self.task_registry,
            num_threads=num_threads,
            error_callback=self.error_callback,
        )

        worker.run()
