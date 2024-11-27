from typing import Callable

from hyrex import constants
from hyrex.hyrex_registry import HyrexRegistry


class HyrexWorker:
    """
    Handles task registration and config for Hyrex worker process.
    """

    def __init__(
        self, queue: str = constants.ANY_QUEUE, error_callback: Callable = None
    ):
        self.queue = queue
        self.error_callback = error_callback
        self.task_registry: HyrexRegistry = HyrexRegistry()

    def add_registry(self, registry: HyrexRegistry):
        for task_name, task_wrapper in registry.items():
            self.task_registry[task_name] = task_wrapper
