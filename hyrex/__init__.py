# from .app import DispatcherType, Hyrex
# from .task_registry import TaskRegistry
import logging

from .hyrex_registry import HyrexRegistry
from .worker import HyrexWorker

# Set up null handler at library root level
logging.getLogger("hyrex").addHandler(logging.NullHandler())
