import os
import signal
import threading
import time
from multiprocessing import Event, Process, Queue

from hyrex.dispatcher import get_dispatcher
from hyrex.logging import get_logger, LogFeature
from hyrex.worker.messages.admin_messages import (ExecutorHeartbeatMessage,
                                                  ExecutorStoppedMessage,
                                                  NewExecutorMessage,
                                                  TaskCanceledMessage,
                                                  TaskHeartbeatMessage)
from hyrex.worker.messages.root_messages import CancelTaskMessage
from hyrex.worker.utils import is_process_alive


class WorkerAdmin(Process):
    def __init__(
        self,
        root_message_queue: Queue,
        admin_message_queue: Queue,
        log_level: str,
    ):
        super().__init__()
        self.log_level = log_level

        self.current_executors = []

        # Outgoing messages to root process
        self.root_message_queue = root_message_queue
        # Incoming messages
        self.admin_message_queue = admin_message_queue
        self._stop_event = Event()

        # To check if root process is running
        self.parent_pid = os.getpid()

    def _message_listener(self):
        while True:
            # Blocking
            message = self.admin_message_queue.get()

            if message == None:
                break

            self.logger.debug(f"Message received: {message}")

            if isinstance(message, NewExecutorMessage):
                self.current_executors.append(message.executor_id)
            elif isinstance(message, ExecutorStoppedMessage):
                # Mark executor as stopped (if not already)
                self.logger.warning(
                    f"Executor {message.executor_id} stopped unexpectedly",
                    feature=LogFeature.DURABILITY,
                    executor_id=str(message.executor_id)
                )
                self.dispatcher.disconnect_executor(message.executor_id)
                self.dispatcher.mark_running_tasks_lost(message.executor_id)
                self.logger.info(
                    f"Marked running tasks as lost for executor {message.executor_id}",
                    feature=LogFeature.DURABILITY,
                    executor_id=str(message.executor_id)
                )
            elif isinstance(message, TaskCanceledMessage):
                self.dispatcher.task_canceled(message.task_id)
            elif isinstance(message, ExecutorHeartbeatMessage):
                self.dispatcher.executor_heartbeat(
                    message.executor_ids, message.timestamp
                )
            elif isinstance(message, TaskHeartbeatMessage):
                self.dispatcher.task_heartbeat(message.task_ids, message.timestamp)

    def run(self):
        # Set log level in environment for any child components
        os.environ["HYREX_LOG_LEVEL"] = self.log_level
        
        # Initialize logger in child process (multiprocessing requirement)
        self.logger = get_logger("admin", LogFeature.PROCESS_MANAGEMENT, level=self.log_level)
        
        self.logger.info("Initializing admin.")
        self.dispatcher = get_dispatcher()

        # Ignore signals, let main process manage shutdown.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, signal.SIG_IGN)

        self.message_listener_thread = threading.Thread(target=self._message_listener)
        self.message_listener_thread.start()
        self.logger.info("Message listener thread now active...")

        try:
            while not self._stop_event.is_set():
                # Get "up_for_cancel" tasks, send to main process
                tasks_to_cancel = self.dispatcher.get_tasks_up_for_cancel()
                for task_id in tasks_to_cancel:
                    self.root_message_queue.put(CancelTaskMessage(task_id=task_id))

                # TODO: Switch to notifications to avoid this sleep.
                self._stop_event.wait(0.5)

                # Confirm parent is still alive
                if not is_process_alive(self.parent_pid):
                    self.logger.warning(
                        "Root process died unexpectedly. Shutting down."
                    )
                    self._stop_event.set()
        finally:
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
            self.logger.error(f"Error during main process shutdown: {e}")
        self.logger.info("Stopping admin")
        self.dispatcher.stop()
