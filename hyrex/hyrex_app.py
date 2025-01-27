from hyrex import constants
from hyrex.hyrex_registry import HyrexRegistry


class HyrexApp:
    """
    Handles task registration and config for running Hyrex workers.
    """

    def __init__(self):
        self.task_registry: HyrexRegistry = HyrexRegistry()

    def add_registry(self, registry: HyrexRegistry):
        self.task_registry.add_registry(registry)
