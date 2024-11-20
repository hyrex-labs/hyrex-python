import logging
import signal
import threading
from datetime import datetime, timezone
from multiprocessing import Process, Queue
from typing import Callable

from uuid_extensions import uuid7

from hyrex import constants
from hyrex.dispatcher import get_dispatcher
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.worker.admin import WorkerAdmin
from hyrex.worker.executor import WorkerExecutor
from hyrex.worker.messages import (
    AdminMessage,
    AdminMessageType,
    RootMessage,
    RootMessageType,
)
from hyrex.worker.logging import LogLevel, init_logging


class WorkerRootProcess:
    def __init__(
        self,
        log_level: LogLevel,
        worker_module_path: str,
        queue: str = None,
        num_processes: int = constants.DEFAULT_EXECUTOR_PROCESSES,
    ):
        self.logger = logging.getLogger(__name__)
        self.log_level = log_level
        init_logging(log_level=log_level)

        self.worker_module_path = worker_module_path
        self.queue = queue
        self.num_processes = num_processes

        self.stop_event = threading.Event()
        self.task_id_to_executor_id: dict[str, str] = {}
        self.executor_id_to_process: dict[str, Process] = {}
        self.executor_processes: list[Process] = []
        self.admin_process: Process = None
        self.root_message_queue = Queue()
        self.admin_message_queue = Queue()

        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            signame = signal.Signals(signum).name
            self.logger.info(f"\nReceived {signame}. Starting graceful shutdown...")
            self.stop_event.set()

        # Register the handler for both SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def cancel_task(self, task_id: str):
        pass

    def set_queue(self, queue: str):
        self.queue = queue

    def set_processes(self, processes: int):
        self.processes = processes

    def _spawn_executor(self):
        executor_id = uuid7()
        executor = WorkerExecutor(
            log_level=self.log_level,
            root_message_queue=self.root_message_queue,
            worker_module_path=self.worker_module_path,
            queue=self.queue,
            executor_id=executor_id,
        )
        executor.start()
        self.executor_processes.append(executor)

    def _spawn_admin(self):
        admin = WorkerAdmin(
            root_message_queue=self.root_message_queue,
            admin_message_queue=self.admin_message_queue,
            log_level=self.log_level,
            queue=self.queue,
        )
        admin.start()
        self.admin_process = admin

    def _message_listener(self):
        while True:
            # Blocking
            raw_message = self.root_message_queue.get()

            if raw_message == None:
                break

            message = RootMessage.model_validate(raw_message)

            if message.type == RootMessageType.CANCEL_TASK:
                pass
            elif message.type == RootMessageType.SET_EXECUTOR_TASK:
                pass
            elif message.type == RootMessageType.HEARTBEAT_REQUEST:
                pass

    def kill_task(self, task_id: str):
        executor_id = self.task_id_to_executor_id.get(task_id)
        if executor_id:
            executor_process = self.executor_id_to_process[executor_id]

    def run(self):
        self.message_listener_thread = threading.Thread(target=self._message_listener)
        self.message_listener_thread.start()

        self.logger.info("Spawning admin process.")
        self._spawn_admin()

        self.logger.info(f"Spawning {self.num_processes} task executor processes.")
        for _ in range(self.num_processes):
            self._spawn_executor()

        while not self.stop_event.is_set():
            self.logger.info("Running action loop")
            self.stop_event.wait(1)

        self.stop()

    def stop(self):
        try:
            # Stop all executors
            self.logger.info("Stopping executor processes.")
            for executor_process in self.executor_processes:
                executor_process._stop_event.set()
                executor_process.join(timeout=constants.WORKER_EXECUTOR_PROCESS_TIMEOUT)
                if executor_process.is_alive():
                    self.logger.warning(
                        "Executor process did not exit cleanly, force killing."
                    )
                    executor_process.kill()
                    executor_process.join(timeout=1.0)

        except Exception as e:
            print(f"Error during executor shutdown: {e}")

        try:
            # Stop admin
            self.logger.info("Stopping admin process.")
            self.admin_process._stop_event.set()
            self.admin_process.join(timeout=constants.WORKER_ADMIN_PROCESS_TIMEOUT)
            if self.admin_process.is_alive():
                self.logger.warning(
                    "Admin process did not exit cleanly, force killing."
                )
                self.admin_process.kill()
                self.admin_process.join(timeout=1.0)

        except Exception as e:
            print(f"Error during admin shutdown: {e}")

        try:
            # Stop internal message listener
            self.root_message_queue.put(None)
            self.message_listener_thread.join()
            if self.message_listener_thread.is_alive():
                self.logger.warning("Message listener thread did not exit cleanly.")

        except Exception as e:
            print(f"Error during main process shutdown: {e}")

        self.logger.info("Worker root process completed.")
