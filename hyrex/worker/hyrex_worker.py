import logging
import threading
from datetime import datetime, timezone
from multiprocessing import Process, Queue
from typing import Callable

from uuid_extensions import uuid7

from hyrex import constants
from hyrex.dispatcher import get_dispatcher
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.task import TaskWrapper
from hyrex.worker.hyrex_admin import HyrexAdmin
from hyrex.worker.hyrex_executor import HyrexExecutor
from hyrex.worker.message_queues import (
    MessageToAdmin,
    MessageToWorker,
    WorkerMessageType,
)


class HyrexWorker:
    def __init__(
        self,
        queue: str = constants.DEFAULT_QUEUE,
        num_processes: int = constants.DEFAULT_EXECUTOR_PROCESSES,
        error_callback: Callable = None,
    ):
        self.logger = logging.getLogger(__name__)

        self.queue = queue
        self.num_processes = num_processes

        self.task_registry: HyrexRegistry = HyrexRegistry(get_dispatcher(worker=True))
        self.error_callback = error_callback

        self.stop_requested = False
        self.task_id_to_executor_id: dict[str, str] = {}
        self.executor_id_to_process: dict[str, Process] = {}
        self.executor_processes: list[Process] = []
        self.admin_process: Process = None
        self.worker_message_queue = Queue()
        self.admin_message_queue = Queue()

    def add_registry(self, registry: HyrexRegistry):
        for task_name, task_wrapper in registry.items():
            self.task_registry[task_name] = task_wrapper

    def _spawn_executor(self):
        executor_id = uuid7()
        executor = HyrexExecutor(queue=self.queue, executor_id=executor_id)
        executor.start()
        self.executor_processes.append(executor)

    def _spawn_admin(self):
        admin = HyrexAdmin(queue=self.queue)
        admin.start()
        self.admin_process = admin

    def _message_listener(self):
        while True:
            # Blocking
            raw_message = self.worker_message_queue.get()

            if raw_message == None:
                break

            message = MessageToWorker.model_validate(raw_message)

            if message.type == WorkerMessageType.EXECUTOR_TASK_UPDATE:
                pass
            elif message.type == WorkerMessageType.HEARTBEAT_REQUEST:
                pass
            elif message.type == WorkerMessageType.TERMINATION_REQUEST:
                pass

    def kill_task(self, task_id: str):
        executor_id = self.task_id_to_executor_id.get(task_id)
        if executor_id:
            executor_process = self.executor_id_to_process[executor_id]

    def run(self):
        self.message_listener_thread = threading.Thread(target=self._message_listener)
        self.message_listener_thread.start()

        self._spawn_admin()

        # for _ in range(self.num_processes):
        #     self._spawn_executor()

        last_heartbeat = datetime.now(timezone=timezone.utc)

        while not self.stop_requested:
            pass

        self.stop()

    def stop(self):
        try:
            # Stop all executors

            # Stop admin
            self.admin_process.terminate()
            self.admin_process.join(timeout=5.0)
            if self.admin_process.is_alive():
                self.logger.warning(
                    "Admin process did not exit cleanly, force killing."
                )
                self.admin_process.kill()
                self.admin_process.join(timeout=1.0)

            # Stop internal message listener
            self.worker_message_queue.put(None)
            self.message_listener_thread.join()
            if self.message_listener_thread.is_alive():
                self.logger.warning("Message listener thread did not exit cleanly.")

        except Exception as e:
            print(f"Error during shutdown: {e}")
