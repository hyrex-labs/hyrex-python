import logging
import signal
import threading
import time
from datetime import datetime, timezone
from multiprocessing import Process, Queue
from uuid import UUID

from uuid_extensions import uuid7

from hyrex import constants
from hyrex.worker.admin import WorkerAdmin
from hyrex.worker.executor import WorkerExecutor
from hyrex.worker.logging import LogLevel, init_logging
from hyrex.worker.messages.admin_messages import (
    ExecutorHeartbeatMessage,
    ExecutorStoppedMessage,
    NewExecutorMessage,
    TaskCanceledMessage,
    TaskHeartbeatMessage,
)
from hyrex.worker.messages.root_messages import (
    CancelTaskMessage,
    HeartbeatRequestMessage,
    SetExecutorTaskMessage,
)


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

        self.heartbeat_requested = False

        self._stop_event = threading.Event()
        self.task_id_to_executor_id: dict[str, str] = {}
        self.executor_id_to_process: dict[str, Process] = {}
        self.admin_process: Process = None
        self.root_message_queue = Queue()
        self.admin_message_queue = Queue()

        self.setup_signal_handlers()

    def setup_signal_handlers(self):
        def signal_handler(signum, frame):
            signame = signal.Signals(signum).name
            self.logger.info(f"\nReceived {signame}. Starting graceful shutdown...")
            self._stop_event.set()

        # Register the handler for both SIGTERM and SIGINT
        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def set_queue(self, queue: str):
        self.queue = queue

    def set_processes(self, processes: int):
        self.processes = processes

    def _spawn_executor(self):
        executor_id = uuid7()
        executor_process = WorkerExecutor(
            log_level=self.log_level,
            root_message_queue=self.root_message_queue,
            worker_module_path=self.worker_module_path,
            queue=self.queue,
            executor_id=executor_id,
        )
        executor_process.start()
        self.executor_id_to_process[executor_id] = executor_process
        # Notify admin of new executor
        self.admin_message_queue.put(NewExecutorMessage(executor_id=executor_id))

    def check_executor_processes(self):
        # Check each executor and respawn if process has died.
        stopped_executors = []
        for executor_id, process in self.executor_id_to_process.items():
            if process.exitcode != None:
                self.logger.info(f"Process {process.pid} stopped. Cleaning up.")
                stopped_executors.append(executor_id)

        for executor_id in stopped_executors:
            # Now clean up everything
            self.executor_id_to_process[executor_id].join(timeout=1.0)
            del self.executor_id_to_process[executor_id]
            # Let message thread clear the task ID mapping to avoid concurrent access issues.
            self.root_message_queue.put(
                SetExecutorTaskMessage(executor_id=executor_id, task_id=None)
            )
            # Notify admin of stopped message
            self.admin_message_queue.put(
                ExecutorStoppedMessage(executor_id=executor_id)
            )

            # Replace the executor process
            self._spawn_executor()

    def _spawn_admin(self):
        admin = WorkerAdmin(
            root_message_queue=self.root_message_queue,
            admin_message_queue=self.admin_message_queue,
            log_level=self.log_level,
            queue=self.queue,
        )
        admin.start()
        self.admin_process = admin

    def check_admin_process(self):
        if not self.admin_process or not self.admin_process.is_alive():
            self.logger.warning("Admin process not running, respawning...")
            if self.admin_process:  # Clean up old process if it exists
                self.admin_process.join(timeout=1.0)
            self._spawn_admin()

    def _message_listener(self):
        while True:
            # Blocking
            message = self.root_message_queue.get()

            if message == None:
                break

            if isinstance(message, CancelTaskMessage):
                self.cancel_running_task(message.task_id)
            elif isinstance(message, SetExecutorTaskMessage):
                self.set_executor_task(
                    executor_id=message.executor_id, task_id=message.task_id
                )
            elif isinstance(message, HeartbeatRequestMessage):
                self.heartbeat_requested = True

    def clear_executor_task(self, executor_id: UUID):
        for k, v in self.task_id_to_executor_id.items():
            if v == executor_id:
                del self.task_id_to_executor_id[k]
                break

    def set_executor_task(self, executor_id: UUID, task_id: UUID):
        self.clear_executor_task(executor_id=executor_id)
        # Add new mapping (unless task_id is None)
        if task_id:
            self.task_id_to_executor_id[task_id] = executor_id

    def cancel_running_task(self, task_id: UUID):
        executor_id = self.task_id_to_executor_id.get(task_id)
        if executor_id:
            executor_process = self.executor_id_to_process[executor_id]
            executor_process.kill()
            self.logger.info(f"Killed executor process to cancel task {task_id}")

            # Notify admin of successful termination
            self.admin_message_queue.put(
                ExecutorStoppedMessage(executor_id=executor_id)
            )
            self.admin_message_queue.put(TaskCanceledMessage(task_id=task_id))

    def send_heartbeats(self):
        self.logger.debug("Sending task and executor heartbeats.")

        self.admin_message_queue.put(
            ExecutorHeartbeatMessage(
                executor_ids=self.executor_id_to_process.keys(),
                timestamp=datetime.now(timezone.utc),
            )
        )
        self.admin_message_queue.put(
            TaskHeartbeatMessage(
                task_ids=self.task_id_to_executor_id.keys(),
                timestamp=datetime.now(timezone.utc),
            )
        )

    def run(self):
        self.message_listener_thread = threading.Thread(target=self._message_listener)
        self.message_listener_thread.start()
        self.logger.info("Incoming message queue now active...")

        self.logger.info("Spawning admin process.")
        self._spawn_admin()

        self.logger.info(f"Spawning {self.num_processes} task executor processes.")
        for _ in range(self.num_processes):
            self._spawn_executor()

        last_heartbeat = time.monotonic()

        try:
            while not self._stop_event.is_set():
                # Check admin and restart if it has died
                self.check_admin_process()

                # Check all executors and restart any that have died
                self.check_executor_processes()

                # Send heartbeat if requested or we're overdue
                current_time = time.monotonic()
                if (
                    self.heartbeat_requested
                    or current_time - last_heartbeat
                    > constants.WORKER_HEARTBEAT_FREQUENCY
                ):
                    self.send_heartbeats()
                    last_heartbeat = time.monotonic()
                    self.heartbeat_requested = False

                # Interruptible sleep
                self._stop_event.wait(1)

        finally:
            self.stop()

    def stop(self):
        try:
            # Stop all executors
            for executor_process in self.executor_id_to_process.values():
                executor_process._stop_event.set()
            for executor_process in self.executor_id_to_process.values():
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
            else:
                self.logger.info("Message listener thread closed successfully.")

        except Exception as e:
            print(f"Error during main process shutdown: {e}")

        self.logger.info("Worker root process completed.")
