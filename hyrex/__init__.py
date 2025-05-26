import logging

from .hyrex_app import HyrexApp
from .hyrex_cache import HyrexCache
from .hyrex_context import HyrexContext, get_hyrex_context, get_hyrex_workflow_context
from .hyrex_queue import HyrexQueue
from .hyrex_registry import HyrexRegistry

# Set up default logging configuration
logger = logging.getLogger("hyrex")
logger.setLevel(logging.INFO)
# Add a console handler if none exists
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
