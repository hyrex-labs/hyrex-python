import threading
from abc import ABC, ABCMeta, abstractmethod
from uuid import UUID

from pydantic import BaseModel

from hyrex import constants
from hyrex.models import HyrexTask, StatusEnum


class SingletonMeta(ABCMeta):
    _instances = {}
    _init_args = {}
    _lock = threading.Lock()

    def __call__(cls, *args, **kwargs):
        # If the instance does not exist, create it and store args
        if cls not in cls._instances:
            with cls._lock:
                if cls not in cls._instances:
                    instance = super().__call__(*args, **kwargs)
                    cls._instances[cls] = instance
                    cls._init_args[cls] = (args, kwargs)
        else:
            # Check if the new args differ from the initial args
            if (args, kwargs) != cls._init_args[cls]:
                raise ValueError(
                    f"Singleton instance already created with "
                    f"different arguments: {cls._init_args[cls]} vs {args, kwargs}"
                )

        return cls._instances[cls]


class DequeuedTask(BaseModel):
    id: UUID
    name: str
    args: dict


class Dispatcher(ABC, metaclass=SingletonMeta):
    # class Dispatcher(ABC):
    @abstractmethod
    def enqueue(
        self,
        task: HyrexTask,
    ):
        pass

    @abstractmethod
    def dequeue(
        self,
        worker_id: UUID,
        queue: str = constants.DEFAULT_QUEUE,
        num_tasks: int = 1,
    ) -> list[DequeuedTask]:
        pass

    @abstractmethod
    def mark_success(self, task_id: UUID):
        pass

    @abstractmethod
    def mark_failed(self, task_id: UUID):
        pass

    @abstractmethod
    def reset_or_cancel_task(self, task_id: UUID):
        pass

    @abstractmethod
    def attempt_retry(self, task_id: UUID):
        pass

    @abstractmethod
    def cancel_task(self, task_id: UUID):
        pass

    # Result must be a JSON string
    @abstractmethod
    def save_result(self, task_id: UUID, result: str):
        pass

    @abstractmethod
    def get_workers_to_cancel(self, worker_ids: list[UUID]) -> list[UUID]:
        pass

    @abstractmethod
    def get_task_status(self, task_id: UUID) -> StatusEnum:
        pass

    @abstractmethod
    def register_worker(self, worker_id: UUID, worker_name: str, queue: str):
        pass

    @abstractmethod
    def mark_worker_stopped(self, worker_id: UUID):
        pass

    @abstractmethod
    def stop(self):
        pass
