import logging
import os
import signal
import socket
import threading
import time
from datetime import datetime, timezone
from multiprocessing import Process, Queue
from uuid import UUID

from uuid6 import uuid7

from hyrex import constants
from hyrex.env_vars import EnvVars
from hyrex.worker.admin import WorkerAdmin
from hyrex.worker.cron_scheduler import WorkerCronScheduler
from hyrex.worker.executor.executor import WorkerExecutor
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
    TaskRegistrationComplete,
)


def generate_worker_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"hyrex-worker-{hostname}-{pid}-{timestamp}"


class WorkerRootProcess:
    def __init__(
        self,
        log_level: LogLevel,
        app_module_path: str,
        queue_pattern: str = None,
        num_processes: int = constants.DEFAULT_EXECUTOR_PROCESSES,
    ):
        self.log_level = log_level
        init_logging(log_level=log_level)
        self.logger = logging.getLogger(__name__)

        self.app_module_path = app_module_path
        self.queue_pattern = queue_pattern
        self.num_processes = num_processes
        self.worker_name = generate_worker_name()

        self.next_executor_number = 1
        # Has an executor registered all current tasks/workflows? Triggers cron scheduler launch
        self.task_registration_complete = False

        self._register_app = True

        self.heartbeat_requested = False

        self.running_on_platform: bool = os.environ.get(EnvVars.API_KEY) is not None

        self._stop_event = threading.Event()
        self.task_id_to_executor_id: dict[str, str] = {}
        self.executor_id_to_process: dict[str, Process] = {}
        self.admin_process: Process = None
        self.cron_scheduler_process: Process = None
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

    # Keep incrementing for each new executor
    def get_next_executor_name(self):
        name = "E" + str(self.next_executor_number)
        self.next_executor_number += 1
        return name

    def _spawn_executor(self):
        executor_id = uuid7()
        executor_process = WorkerExecutor(
            log_level=self.log_level,
            root_message_queue=self.root_message_queue,
            app_module_path=self.app_module_path,
            queue=self.queue_pattern,
            executor_id=executor_id,
            register_app=self._register_app,
            worker_name=self.worker_name,
            executor_name=self.get_next_executor_name(),
        )
        # Only register app/tasks once per worker.
        if self._register_app:
            self._register_app = False
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
        )
        admin.start()
        self.admin_process = admin

    def check_admin_process(self):
        if not self.admin_process or not self.admin_process.is_alive():
            self.logger.warning("Admin process not running, respawning...")
            if self.admin_process:  # Clean up old process if it exists
                self.admin_process.join(timeout=1.0)
            self._spawn_admin()

    def _spawn_cron_scheduler(self):
        cron_scheduler = WorkerCronScheduler(
            log_level=self.log_level, worker_name=self.worker_name
        )
        cron_scheduler.start()
        self.cron_scheduler_process = cron_scheduler

    def check_cron_scheduler_process(self):
        if (
            not self.cron_scheduler_process
            or not self.cron_scheduler_process.is_alive()
        ):
            self.logger.warning("Cron scheduler process not running, respawning...")
            if self.cron_scheduler_process:
                self.cron_scheduler_process.join(timeout=1.0)
            self._spawn_cron_scheduler()

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
            elif isinstance(message, TaskRegistrationComplete):
                self.task_registration_complete = True
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

        self.logger.info("Waiting for executor to complete task registration.")
        while not self.task_registration_complete and not self._stop_event.is_set():
            time.sleep(0.5)

        # Hyrex Cloud handles cron scheduling
        if not self.running_on_platform:
            self.logger.info("Spawning cron scheduler process.")
            self._spawn_cron_scheduler()

        last_heartbeat = time.monotonic()

        try:
            while not self._stop_event.is_set():
                # Check admin and restart if it has died
                self.check_admin_process()

                # Hyrex Cloud handles cron scheduling
                if not self.running_on_platform:
                    # Check cron scheduler and restart if it has died
                    self.check_cron_scheduler_process()

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

        # Hyrex Cloud handles cron scheduling
        if not self.running_on_platform:
            try:
                self.logger.info("Stopping cron scheduler process.")
                self.cron_scheduler_process._stop_event.set()
                self.cron_scheduler_process.join(
                    timeout=constants.WORKER_CRON_SCHEDULER_PROCESS_TIMEOUT
                )
                if self.cron_scheduler_process.is_alive():
                    self.logger.warning(
                        "Cron scheduler process did not exit cleanly, force killing."
                    )
                    self.cron_scheduler_process.kill()
                    self.cron_scheduler_process.join(timeout=1.0)
            except Exception as e:
                print(f"Error during cron scheduler shutdown: {e}")

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
