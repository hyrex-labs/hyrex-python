from abc import ABC, abstractmethod
from uuid import UUID

from pydantic import BaseModel

from hyrex.models import HyrexTask


class Dispatcher(ABC):
    @abstractmethod
    def enqueue(
        self,
        task: HyrexTask,
    ):
        pass

    @abstractmethod
    def dequeue(self):
        pass

    @abstractmethod
    def wait(self, task_id: UUID):
        pass

    @abstractmethod
    def retrieve_status(self, task_id: UUID):
        pass
