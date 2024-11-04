# import functools
# import logging
# import os
# import signal
# from enum import Enum
# from typing import Any, Callable
# from uuid import UUID

# from uuid_extensions import uuid7

# from hyrex import constants

# # from hyrex.async_worker import AsyncWorker
# from hyrex.dispatcher import (
#     PlatformDispatcher,
#     PostgresDispatcher,
#     PostgresLiteDispatcher,
# )
# from hyrex.task import TaskWrapper
# from hyrex.task_registry import TaskRegistry
# from hyrex.worker import Worker
# from hyrex.worker_manager import WorkerManager


# class EnvVars:
#     DATABASE_URL = "HYREX_DATABASE_URL"
#     API_KEY = "HYREX_API_KEY"
#     PLATFORM_URL = "HYREX_PLATFORM_URL"


# class DispatcherType(Enum):
#     POSTGRES = 1
#     PLATFORM = 2
#     POSTGRES_LITE = 3


# class Hyrex:
#     PLATFORM_URL = os.getenv(EnvVars.PLATFORM_URL)

#     def __init__(
#         self,
#         app_id: str,
#         dispatcher_type: DispatcherType = DispatcherType.POSTGRES,
#         conn: str = os.getenv(EnvVars.DATABASE_URL),
#         api_key: str = os.getenv(EnvVars.API_KEY),
#         error_callback: Callable = None,
#     ):
#         self.app_id = app_id
#         self.conn = conn
#         self.api_key = api_key

#         self.dispatcher = self._init_dispatcher(dispatcher_type)

#         self._setup_signal_handlers()

#         self.error_callback = error_callback
#         self.task_registry = TaskRegistry()

#     def _init_dispatcher(self, dispatcher_type: DispatcherType):
#         if dispatcher_type == DispatcherType.POSTGRES:
#             if self.conn == None:
#                 raise ValueError(
#                     "Hyrex Postgres dispatcher requires a connection string. Have you set HYREX_DATABASE_URL?"
#                 )
#             return PostgresDispatcher(conn_string=self.conn)
#         elif dispatcher_type == DispatcherType.PLATFORM:
#             if self.api_key == None:
#                 raise ValueError(
#                     "Hyrex Platform dispatcher requires an API key. Have you set HYREX_API_KEY?"
#                 )
#             return PlatformDispatcher(api_key=self.api_key)
#         elif dispatcher_type == DispatcherType.POSTGRES_LITE:
#             if self.conn == None:
#                 raise ValueError(
#                     "Hyrex Postgres dispatcher requires a connection string. Have you set HYREX_DATABASE_URL?"
#                 )
#             return PostgresLiteDispatcher(conn_string=self.conn)

#         else:
#             raise NotImplementedError("Dispatcher type not yet implemented.")

#     def stop(self):
#         self.dispatcher.stop()

#     def _signal_handler(self, signum, frame):
#         logging.info("SIGTERM received, stopping Hyrex dispatcher...")
#         self.dispatcher.stop()

#     def _chain_signal_handlers(self, new_handler, old_handler):
#         """Return a function that calls both the new and old signal handlers."""

#         def wrapper(signum, frame):
#             # Call the new handler first
#             new_handler(signum, frame)
#             # Then call the previous handler (if it exists)
#             if old_handler and callable(old_handler):
#                 old_handler(signum, frame)

#         return wrapper

#     def _setup_signal_handlers(self):
#         for sig in (signal.SIGTERM, signal.SIGINT):
#             old_handler = signal.getsignal(sig)  # Get the existing handler
#             new_handler = self._signal_handler  # Your new handler
#             # Set the new handler, which calls both new and old handlers
#             signal.signal(sig, self._chain_signal_handlers(new_handler, old_handler))

#     def task(
#         self,
#         func: Callable = None,
#         *,
#         queue: str = constants.DEFAULT_QUEUE,
#         cron: str = None,
#         max_retries: int = 0,
#         priority: int = constants.DEFAULT_PRIORITY
#     ) -> TaskWrapper:
#         """
#         Task decorator
#         """
#         if func is None:
#             # The decorator is used with arguments: @hy.task(max_retries=1)
#             def decorator(func):
#                 task_wrapper = self.task_registry.task(
#                     func,
#                     queue=queue,
#                     cron=cron,
#                     max_retries=max_retries,
#                     priority=priority,
#                 )
#                 self.task_registry.set_dispatcher(self.dispatcher)
#                 return task_wrapper

#             return decorator
#         else:
#             # The decorator is used without arguments: @hy.task
#             task_wrapper = self.task_registry.task(
#                 func, queue=queue, cron=cron, max_retries=max_retries, priority=priority
#             )
#             self.task_registry.set_dispatcher(self.dispatcher)
#             return task_wrapper

#     def add_registry(self, registry: TaskRegistry):
#         self.task_registry.add_registry(registry)
#         self.task_registry.set_dispatcher(self.dispatcher)

#     def schedule(self):
#         self.task_registry.schedule()

#     def run_worker(
#         self,
#         worker_id: UUID = None,
#         queue: str = constants.DEFAULT_QUEUE,
#         log_level: int = logging.INFO,
#     ):
#         logging.basicConfig(level=log_level)

#         worker = Worker(
#             dispatcher=self.dispatcher,
#             queue=queue,
#             worker_id=worker_id,
#             task_registry=self.task_registry,
#             error_callback=self.error_callback,
#         )

#         worker.run()

#     def run_manager(
#         self,
#         app_module: str,
#         num_workers: int,
#         queue: str = constants.DEFAULT_QUEUE,
#         log_level: int = logging.INFO,
#     ):
#         logging.basicConfig(level=log_level)

#         manager = WorkerManager(
#             app_module=app_module,
#             queue=queue,
#             num_workers=num_workers,
#             dispatcher=self.dispatcher,
#         )

#         manager.run()
