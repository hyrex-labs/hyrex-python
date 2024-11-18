import logging
import time
from multiprocessing import Process, Queue

from hyrex.dispatcher import get_dispatcher


class HyrexAdmin(Process):
    def __init__(self, queue: str):
        super().__init__()
        self.logger = logging.getLogger(__name__)

        self.queue = queue
        self.stop_requested = False

    def run(self):
        self.dispatcher = get_dispatcher()

        time.sleep(1)
        pass

        # Get "up_for_cancel" tasks, send to main process
        tasks_to_cancel = []

        # Check canceled tasks, mark as canceled

        # Check closed worker list, mark as stopped

        # Check for heartbeat request

        # Relay heartbeat
