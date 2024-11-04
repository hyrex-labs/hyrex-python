# from hyrex.decorator import TaskDecoratorProvider
import functools
import logging
import signal
from typing import Any, Callable

from hyrex import constants
from hyrex.dispatcher import get_dispatcher
from hyrex.task import T, TaskWrapper


class HyrexRegistry(dict[str, "TaskWrapper"]):
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

    def _signal_handler(self, signum, frame):
        logging.info("SIGTERM received, stopping Hyrex dispatcher...")
        self.dispatcher.stop()

    def _chain_signal_handlers(self, new_handler, old_handler):
        """Return a function that calls both the new and old signal handlers."""

        def wrapper(signum, frame):
            # Call the new handler first
            new_handler(signum, frame)
            # Then call the previous handler (if it exists)
            if old_handler and callable(old_handler):
                old_handler(signum, frame)

        return wrapper

    def _setup_signal_handlers(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handler = signal.getsignal(sig)  # Get the existing handler
            new_handler = self._signal_handler  # Your new handler
            # Set the new handler, which calls both new and old handlers
            signal.signal(sig, self._chain_signal_handlers(new_handler, old_handler))

    def __init__(self):
        self.dispatcher = get_dispatcher()
        self._setup_signal_handlers()

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
            return wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def schedule(self):
        for task_wrapper in self.values():
            task_wrapper.schedule()
