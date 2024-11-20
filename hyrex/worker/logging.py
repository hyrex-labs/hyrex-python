import logging


def init_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter(
            "[PID: %(process)d] %(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
    )
    logger = logging.getLogger("hyrex")
    # logger.setLevel(level=getattr(logging, log_level.upper()))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
