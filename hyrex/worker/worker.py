from typing import Any, Callable

from hyrex import constants
from hyrex.hyrex_registry import HyrexRegistry


class HyrexWorker:
    """
    Handles task registration and config for Hyrex worker process.
    """

    def __init__(
        self, queue_pattern: str = constants.ANY_QUEUE, error_callback: Callable = None
    ):
        self.queue_pattern = queue_pattern
        self.task_registry: HyrexRegistry = HyrexRegistry()

    def add_registry(self, registry: HyrexRegistry):
        self.task_registry.add_registry(registry)
