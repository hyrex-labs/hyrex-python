import logging
import os
import signal
import time
import threading
from multiprocessing import Event, Process, Queue

from hyrex.dispatcher import get_dispatcher
from hyrex.worker.logging import LogLevel, init_logging
from hyrex.worker.messages import (
    AdminMessage,
    AdminMessageType,
)


class WorkerAdmin(Process):
    def __init__(
        self,
        root_message_queue: Queue,
        admin_message_queue: Queue,
        log_level: LogLevel,
        queue: str,
    ):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.log_level = log_level

        # Hyrex queue for tasks
        self.queue = queue

        # Outgoing messages to root process
        self.root_message_queue = root_message_queue
        # Incoming messages
        self.admin_message_queue = admin_message_queue
        self._stop_event = Event()

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            signame = signal.Signals(signum).name
            self.logger.info(f"\nReceived {signame}. Starting graceful shutdown...")
            self._stop_event.set()

        # Register the handler for both SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _message_listener(self):
        while True:
            # Blocking
            raw_message = self.admin_message_queue.get()

            if raw_message == None:
                break

            message = AdminMessage.model_validate(raw_message)

            if message.message_type == AdminMessageType.NEW_EXECUTOR:
                pass

    def run(self):
        os.setpgrp()
        init_logging(self.log_level)

        self.logger.info("Initializing admin.")
        self.dispatcher = get_dispatcher(worker=True)
        self.setup_signal_handlers()

        self.message_listener_thread = threading.Thread(target=self._message_listener)
        self.message_listener_thread.start()
        self.logger.info("Message listener thread now active...")

        while not self._stop_event.is_set():

            # Get "up_for_cancel" tasks, send to main process
            tasks_to_cancel = []

            # Check for heartbeat request

            self._stop_event.wait(1)

        self.stop()

    def stop(self):
        try:
            # Stop internal message listener
            self.admin_message_queue.put(None)
            self.message_listener_thread.join()
            if self.message_listener_thread.is_alive():
                self.logger.warning("Message listener thread did not exit cleanly.")
            else:
                self.logger.info("Message listener thread closed successfully.")

        except Exception as e:
            print(f"Error during main process shutdown: {e}")
        self.logger.info("Stopping admin")
        self.dispatcher.stop()
