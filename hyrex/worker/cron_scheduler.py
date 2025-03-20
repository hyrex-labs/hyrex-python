import logging
import os
import signal
from datetime import datetime, timezone
from multiprocessing import Event, Process

from croniter import croniter

from hyrex.dispatcher import CronJob, get_dispatcher
from hyrex.dispatcher.dispatcher import CronJobRun
from hyrex.worker.logging import LogLevel, init_logging
from hyrex.worker.utils import is_process_alive

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

    def check_stop_conditions(self):
        # Confirm parent is still alive
        if not is_process_alive(self.parent_pid):
            self.logger.warning("Root process died unexpectedly. Shutting down.")
            self._stop_event.set()

    def acquire_scheduler_lock(self) -> int | None:
        self.logger.info("Acquiring cron scheduler lock...")
        result = self.dispatcher.acquire_scheduler_lock(self.worker_name)
        return result

    def update_cron_confirmation_timestamp_to_now(self, cron_job_id: int):
        self.dispatcher.update_cron_job_confirmation_timestamp(cron_job_id)

    def impute_scheduled_cron_job_runs(self, cron_job: CronJob) -> list[CronJobRun]:
        # Create iterator starting from the last confirmed date or activation date
        start_time = max(cron_job.activated_at, cron_job.scheduled_jobs_confirmed_until)
        iterator = croniter(cron_job.schedule, start_time=start_time)

        cron_job_runs = []
        now = datetime.now(timezone.utc)  # Create timezone-aware UTC datetime

        next_interval_date = iterator.get_next(datetime)
        # Ensure next_interval_date is timezone-aware with UTC
        if next_interval_date.tzinfo is None:
            next_interval_date = next_interval_date.replace(tzinfo=timezone.utc)

        while next_interval_date <= now:
            cron_job_runs.append(
                CronJobRun(
                    jobid=cron_job.jobid,
                    command=cron_job.command,
                    schedule_time=next_interval_date,
                )
            )

            next_interval_date = iterator.get_next(datetime)
            if next_interval_date.tzinfo is None:
                next_interval_date = next_interval_date.replace(tzinfo=timezone.utc)

        return cron_job_runs

    def run(self):
        init_logging(self.log_level)

        self.logger.info("Initializing cron scheduler.")
        self.dispatcher = get_dispatcher(worker=True)

        # Ignore signals, let main process manage shutdown.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.lock_id = None

        try:
            while not self._stop_event.is_set() and not self.lock_id:
                self.check_stop_conditions()

                self.lock_id = self.acquire_scheduler_lock()
                if not self.lock_id and not self._stop_event.is_set():
                    self.logger.info(
                        "Failed to acquire lock. Trying again in 15 seconds."
                    )
                    # Sleep and then try again
                    self._stop_event.wait(15)
                    continue

            # If we're stopping, exit early
            if self._stop_event.is_set():
                self.logger.info("Stop event detected during lock acquisition.")
                return

            self.logger.info("Acquired lock.")

            # Decide whether to backfill cron jobs
            cron_expressions = self.dispatcher.pull_cron_job_expressions()
            for cron_job in cron_expressions:
                if not cron_job.should_backfill:
                    self.update_cron_confirmation_timestamp_to_now(cron_job.jobid)

            # Main loop with lock held
            while not self._stop_event.is_set():
                self.check_stop_conditions()

                # TODO: Update lock heartbeat

                cron_expressions = self.dispatcher.pull_cron_job_expressions()

                # Queue cron job runs
                for cron_job in cron_expressions:
                    self.logger.info(
                        f"Got cron job {cron_job.jobname}, confirmed_until={cron_job.scheduled_jobs_confirmed_until}"
                    )
                    scheduled_jobs = self.impute_scheduled_cron_job_runs(cron_job)
                    self.dispatcher.schedule_cron_job_runs(scheduled_jobs)

                # Execute cron job runs
                result = self.dispatcher.execute_queued_cron_job_run()
                while result and result == "executed":
                    self.logger.info("Executed cron job run...")
                    result = self.dispatcher.execute_queued_cron_job_run()

                self._stop_event.wait(LOOP_RATE_SECONDS)
        finally:
            self.stop()

    def stop(self):
        self.logger.info("Stopping cron scheduler.")
        if self.lock_id:
            self.logger.info("Releasing scheduler lock...")
            self.dispatcher.release_scheduler_lock(self.worker_name)
            self.lock_id = None
        self.dispatcher.stop()
