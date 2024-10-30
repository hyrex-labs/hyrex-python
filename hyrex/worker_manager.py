import logging
import os
import signal
import subprocess
import time

from uuid_extensions import uuid7

from hyrex import constants


class WorkerManager:
    def __init__(
        self,
        app: str,
        queue: str = constants.DEFAULT_QUEUE,
        num_workers: int = 8,
        log_level: int = logging.INFO,
    ):
        self.app = app
        self.queue = queue
        self.num_workers = num_workers
        self.worker_map = {}
        self._stop_requested = False

        logging.basicConfig(level=log_level)

    def stop(self):
        for worker_process in self.worker_map.values():
            worker_process.terminate()

        for worker_id, worker_process in self.worker_map.items():
            try:
                worker_process.wait(timeout=5)
            except:
                logging.warning(
                    f"Worker ID {worker_id} did not terminate. Stopping forcefully."
                )
                worker_process.kill()

        logging.info("All worker processes successfully stopped.")

    def _signal_handler(self, signum, frame):
        logging.info("SIGTERM received by worker manager. Beginning shutdown.")
        self._stop_requested = True

    def add_new_worker_process(self):
        worker_id = uuid7()
        self.worker_map[worker_id] = subprocess.Popen(
            [
                "hyrex",
                "worker-process",
                self.app,
                "--worker-id",
                str(worker_id),
            ],
            preexec_fn=os.setsid,
        )

    def run(self):
        # Note: This overrides the Hyrex instance signal handler,
        # which makes the worker manager responsible for stopping the dispatcher.
        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, self._signal_handler)

        logging.info("Spinning up worker processes.")

        try:
            for i in range(self.num_workers):
                self.add_new_worker_process()

            while not self._stop_requested:
                # Poll for canceled tasks

                # Check for lost workers
                for worker_id, worker_process in list(self.worker_map.items()):
                    retcode = worker_process.poll()
                    if retcode is not None:
                        logging.warning(
                            f"Worker process {worker_id} exited with code {retcode}"
                        )
                        del self.worker_map[worker_id]
                        # Replace the exited worker.
                        logging.info("Creating new worker process.")
                        self.add_new_worker_process()

                time.sleep(1)

        finally:
            self.stop()
