from pydantic import BaseModel

from hyrex import constants
from hyrex.hyrex_registry import HyrexRegistry


class HyrexAppInfo(BaseModel):
    name: str


class HyrexApp:
    """
    Handles task registration and config for running Hyrex workers.
    """

    def __init__(self, app_name: str = "HelloHyrex"):
        self.task_registry: HyrexRegistry = HyrexRegistry()
        self.app_info = HyrexAppInfo(name=app_name)

    def add_registry(self, registry: HyrexRegistry):
        self.task_registry.add_registry(registry)
