import logging
from enum import StrEnum


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
