import functools
import logging
import os
from typing import Any, Callable

from hyrex import constants
from hyrex.async_worker import AsyncWorker
from hyrex.task import TaskWrapper
from hyrex.task_registry import TaskRegistry


class EnvVars:
    DATABASE_URL = "HYREX_DATABASE_URL"
    API_KEY = "HYREX_API_KEY"
    PLATFORM_URL = "HYREX_PLATFORM_URL"


class Hyrex:
    PLATFORM_URL = os.getenv(EnvVars.PLATFORM_URL)

    def __init__(
        self,
        app_id: str,
        conn: str = os.getenv(EnvVars.DATABASE_URL),
        api_key: str = os.getenv(EnvVars.API_KEY),
        error_callback: Callable = None,
    ):
        self.app_id = app_id
        self.conn = conn
        self.api_key = api_key

        if not self.conn and not self.api_key:
            raise ValueError("Hyrex requires a connection string or an API key to run.")

        if self.api_key and not self.PLATFORM_URL:
            raise ValueError(
                "Hyrex requires a HYREX_PLATFORM_URL if API key is provided."
            )

        self.error_callback = error_callback
        self.task_registry = TaskRegistry()

    def task(
        self,
        func: Callable = None,
        *,
        queue: str = constants.DEFAULT_QUEUE,
        cron: str = None,
        max_retries: int = 0,
        priority: int = constants.DEFAULT_PRIORITY
    ) -> TaskWrapper:
        """
        Task decorator
        """
        if func is None:
            # The decorator is used with arguments: @hy.task(max_retries=1)
            def decorator(func):
                task_wrapper = self.task_registry.task(
                    func,
                    queue=queue,
                    cron=cron,
                    max_retries=max_retries,
                    priority=priority,
                )
                self.task_registry.set_connection(
                    self.conn, self.api_key, self.PLATFORM_URL
                )
                return task_wrapper

            return decorator
        else:
            # The decorator is used without arguments: @hy.task
            task_wrapper = self.task_registry.task(
                func, queue=queue, cron=cron, max_retries=max_retries, priority=priority
            )
            self.task_registry.set_connection(
                self.conn, self.api_key, self.PLATFORM_URL
            )
            return task_wrapper

    def add_registry(self, registry: TaskRegistry):
        self.task_registry.add_registry(registry)
        self.task_registry.set_connection(self.conn, self.api_key, self.PLATFORM_URL)

    def schedule(self):
        self.task_registry.schedule()

    def run_worker(
        self,
        queue: str = constants.DEFAULT_QUEUE,
        num_threads: int = 8,
        log_level: int = logging.INFO,
    ):
        logging.basicConfig(level=log_level)

        worker = AsyncWorker(
            conn=self.conn,
            api_key=self.api_key,
            api_base_url=self.PLATFORM_URL,
            queue=queue,
            task_registry=self.task_registry,
            num_threads=num_threads,
            error_callback=self.error_callback,
        )

        worker.run()
