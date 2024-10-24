from abc import ABC, abstractmethod
from uuid import UUID

from pydantic import BaseModel

from hyrex import constants
from hyrex.models import HyrexTask, StatusEnum


class DequeuedTask(BaseModel):
    id: UUID
    name: str
    args: dict


class Dispatcher(ABC):
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
    def reset_task(self, task_id: UUID):
        pass

    @abstractmethod
    def attempt_retry(self, task_id: UUID):
        pass

    @abstractmethod
    def cancel_task(self, task_id: UUID):
        pass

    @abstractmethod
    def get_task_status(self, task_id: UUID) -> StatusEnum:
        pass

    @abstractmethod
    def register_worker(self, worker_id: UUID):
        pass

    @abstractmethod
    def mark_worker_stopped(self, worker_id: UUID):
        pass

    @abstractmethod
    def stop(self):
        pass
