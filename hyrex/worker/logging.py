import logging
from enum import StrEnum


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


def init_logging(log_level: str):
    logger = logging.getLogger("hyrex")
    
    # Clear any existing handlers to prevent duplicates
    logger.handlers.clear()
    
    # Add our PID-formatted handler
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[PID: %(process)d] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    logger.addHandler(handler)
    logger.setLevel(level=getattr(logging, log_level.upper()))
    
    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False
