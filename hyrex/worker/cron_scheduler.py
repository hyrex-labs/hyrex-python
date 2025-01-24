import logging
import os
import signal
import threading
import time
from multiprocessing import Event, Process, Queue

from hyrex.dispatcher import get_dispatcher, CronJob
from hyrex.worker.logging import LogLevel, init_logging
from hyrex.worker.messages.root_messages import CancelTaskMessage
from hyrex.worker.utils import is_process_alive
from pydantic import BaseModel
from datetime import datetime


DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10
LOOP_RATE_SECONDS = 30


class WorkerCronScheduler(Process):
    def __init__(self, log_level: LogLevel, worker_name: str):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.log_level = log_level
        self.worker_name = worker_name

        self._stop_event = Event()

        # To check if root process is running
        self.parent_pid = os.getpid()

    def acquire_scheduler_lock(self) -> int | None:
        self.logger.info("Acquiring cron scheduler lock...")
        result = self.dispatcher.acquire_scheduler_lock(self.worker_name)
        return result

    def update_cron_confirmation_timestamp_to_now(self, cron_job: CronJob):
        self.dispatcher.update_cron_confirmation_timestamp(cron_job.jobid)

    # def cron_scheduler_loop(self):

    def run(self):
        init_logging(self.log_level)

        self.logger.info("Initializing cron scheduler.")
        self.dispatcher = get_dispatcher(worker=True)

        # Ignore signals, let main process manage shutdown.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.lock_id = None

        try:
            # TODO: Handle parent process dying in this loop.
            while not self.lock_id:
                self.lock_id = self.acquire_scheduler_lock()
                if not self.lock_id:
                    self.logger.info(
                        "Failed to acquire lock. Trying again in 15 seconds."
                    )
                    # Sleep and then try again
                    self._stop_event.wait(15)
                    continue

            self.logger.info("Acquired lock.")

            # Decide whether to backfill cron jobs
            cron_expressions = self.dispatcher.pull_cron_job_expressions()
            for cron_job in cron_expressions:
                if not cron_job.should_backfill:
                    self.update_cron_confirmation_timestamp_to_now(cron_job.job_id)

            # HERE SO FAR

            # Main loop with lock held
            while not self._stop_event().is_set():

                # Confirm parent is still alive
                if not is_process_alive(self.parent_pid):
                    self.logger.warning(
                        "Root process died unexpectedly. Shutting down."
                    )
                    self._stop_event.set()

                self._stop_event.wait()
        finally:
            self.stop()

    def stop(self):
        self.logger.info("Stopping cron scheduler.")
        self.dispatcher.stop()
