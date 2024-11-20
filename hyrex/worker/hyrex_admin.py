import logging
import os
import signal
import time
from multiprocessing import Event, Process, Queue

from hyrex.dispatcher import get_dispatcher


class HyrexAdmin(Process):
    def __init__(self, queue: str):
        super().__init__()
        self.logger = logging.getLogger(__name__)

        self.queue = queue
        self._stop_event = Event()
        self.start_method = "spawn"

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            signame = signal.Signals(signum).name
            self.logger.info(f"\nReceived {signame}. Starting graceful shutdown...")
            self._stop_event.set()

        # Register the handler for both SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def run(self):
        os.setpgrp()
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

        self.logger.info("Starting HyrexAdmin process.")
        self.dispatcher = get_dispatcher(worker=True)
        self.setup_signal_handlers()

        while not self._stop_event.is_set():
            self._stop_event.wait(1)
            self.logger.info("Running admin loop")

            # Get "up_for_cancel" tasks, send to main process
            tasks_to_cancel = []

            # Check canceled tasks, mark as canceled

            # Check closed worker list, mark as stopped

            # Check for heartbeat request

            # Relay heartbeat

        self.logger.info("Stopping admin")
        self.dispatcher.stop()
        # self.stop()
