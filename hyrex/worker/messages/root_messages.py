from enum import StrEnum
from typing import Union
from uuid import UUID

from pydantic import BaseModel


class CancelTaskMessage(BaseModel):
    task_id: UUID


class SetExecutorTaskMessage(BaseModel):
    executor_id: UUID
    task_id: UUID | None


class TaskRegistrationComplete(BaseModel):
    pass


class HeartbeatRequestMessage(BaseModel):
    pass
