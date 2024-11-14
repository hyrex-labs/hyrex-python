import logging
import os
import signal
import subprocess
import time
from uuid import UUID

from uuid_extensions import uuid7

from hyrex import constants
from hyrex.dispatcher import get_dispatcher


class WorkerNode:
    def __init__(
        self,
        app_module: str,
        queue: str = constants.DEFAULT_QUEUE,
        num_workers: int = 8,
        log_level: str = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.log_level = log_level  # For passing log level on to worker processes

        self.app_module = app_module
        self.dispatcher = get_dispatcher()
        self.queue = queue
        self.num_workers = num_workers
        self.worker_map = {}
        self._stop_requested = False

    def terminate_worker(self, worker_id: UUID):
        if worker_id not in self.worker_map:
            self.logger.warning(
                f"Tried to terminate untracked worker with ID: {worker_id}"
            )
            return

        worker_process = self.worker_map[worker_id]
        worker_process.terminate()
        try:
            worker_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.logger.warning(
                f"Worker ID {worker_id} did not terminate. Stopping forcefully."
            )
            worker_process.kill()

        del self.worker_map[worker_id]

    def stop(self):
        for worker_process in self.worker_map.values():
            worker_process.terminate()

        for worker_id, worker_process in self.worker_map.items():
            try:
                worker_process.wait(timeout=5)
            except:
                self.logger.warning(
                    f"Worker ID {worker_id} did not terminate. Stopping forcefully."
                )
                worker_process.kill()

        self.logger.info("All worker processes successfully stopped.")

        self.dispatcher.stop()

        self.logger.info("Manager shutdown successful.")

    def _signal_handler(self, signum, frame):
        self.logger.info("SIGTERM received by worker manager. Beginning shutdown.")
        self._stop_requested = True

    def add_new_worker_process(self):
        worker_id = uuid7()
        try:
            self.worker_map[worker_id] = subprocess.Popen(
                [
                    "hyrex",
                    "worker-process",
                    self.app_module,
                    "--worker-id",
                    str(worker_id),
                    "--log-level",
                    self.log_level,
                ],
                preexec_fn=os.setsid,
            )
        except Exception as e:
            self.logger.error(f"Failed to start worker {worker_id}: {e}")

    def run(self):
        # Note: This overrides the Hyrex instance signal handler,
        # which makes the manager responsible for stopping the dispatcher.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

        self.logger.info("Spinning up worker processes.")

        for i in range(self.num_workers):
            self.add_new_worker_process()

        try:
            while not self._stop_requested:
                # Poll for workers running canceled tasks
                workers_to_terminate = self.dispatcher.get_workers_to_cancel(
                    list(self.worker_map.keys())
                )
                for worker_id in workers_to_terminate:
                    self.logger.info(
                        f"Terminating worker {worker_id} to cancel running task."
                    )
                    self.terminate_worker(worker_id)
                    self.add_new_worker_process()

                # Check for exited worker processes
                for worker_id, worker_process in list(self.worker_map.items()):
                    retcode = worker_process.poll()
                    if retcode is not None:
                        self.logger.warning(
                            f"Worker process {worker_id} exited with code {retcode}"
                        )
                        del self.worker_map[worker_id]
                        # Replace the exited worker.
                        self.logger.info("Creating new worker process.")
                        self.add_new_worker_process()

                time.sleep(1)

        finally:
            self.stop()