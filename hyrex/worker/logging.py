import logging
import sys
from enum import Enum

# Python 3.9 compatibility - StrEnum was introduced in Python 3.11
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    class StrEnum(str, Enum):
        """Compatibility shim for Python < 3.11"""
        pass


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def init_logging(log_level: str):
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[PID: %(process)d] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    logger = logging.getLogger("hyrex")
    logger.setLevel(level=getattr(logging, log_level.upper()))
    logger.addHandler(handler)
