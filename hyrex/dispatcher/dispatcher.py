import atexit
import logging
import signal
from abc import ABC, abstractmethod
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from hyrex import constants
from hyrex.models import HyrexTask, StatusEnum


class DequeuedTask(BaseModel):
    id: UUID
    name: str
    args: dict


class Dispatcher(ABC):
    logger = logging.getLogger(__name__)

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        self.logger.debug(f"\nReceived {signame}. Shutting down Hyrex dispatcher...")
        self.stop()

    def _chain_signal_handlers(self, new_handler, old_handler):
        """Return a function that calls both the new and old signal handlers."""

        def wrapper(signum, frame):
            # Call the new handler first
            new_handler(signum, frame)
            # Then call the previous handler (if it exists)
            if old_handler and callable(old_handler):
                old_handler(signum, frame)

        return wrapper

    def _setup_signal_handlers(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handler = signal.getsignal(sig)  # Get the existing handler
            new_handler = self._signal_handler  # Your new handler
            # Set the new handler, which calls both new and old handlers
            signal.signal(sig, self._chain_signal_handlers(new_handler, old_handler))

    def register_shutdown_handlers(self):
        self._setup_signal_handlers()
        atexit.register(self.stop)

    def __init__(self):
        pass

    @abstractmethod
    def enqueue(
        self,
        task: HyrexTask,
    ):
        pass

    @abstractmethod
    def dequeue(
        self,
        executor_id: UUID,
        queue: str = constants.ANY_QUEUE,
    ) -> DequeuedTask:
        pass

    @abstractmethod
    def mark_success(self, task_id: UUID):
        pass

    @abstractmethod
    def mark_failed(self, task_id: UUID):
        pass

    @abstractmethod
    def attempt_retry(self, task_id: UUID):
        pass

    @abstractmethod
    def try_to_cancel_task(self, task_id: UUID):
        pass

    @abstractmethod
    def task_canceled(self, task_id: UUID):
        pass

    # Result must be a JSON string
    @abstractmethod
    def save_result(self, task_id: UUID, result: str):
        pass

    # @abstractmethod
    # def get_workers_to_cancel(self, worker_ids: list[UUID]) -> list[UUID]:
    #     pass

    @abstractmethod
    def get_task_status(self, task_id: UUID) -> StatusEnum:
        pass

    @abstractmethod
    def register_executor(self, executor_id: UUID, executor_name: str, queue: str):
        pass

    @abstractmethod
    def disconnect_executor(self, executor_id: UUID):
        pass

    @abstractmethod
    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        pass

    @abstractmethod
    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        pass

    @abstractmethod
    def get_tasks_up_for_cancel(self) -> list[UUID]:
        pass

    @abstractmethod
    def get_queues_for_pattern(self, pattern: str) -> list[str]:
        pass

    @abstractmethod
    def stop(self):
        pass
