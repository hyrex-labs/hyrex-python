from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class NewExecutorMessage(BaseModel):
    executor_id: UUID


class ExecutorStoppedMessage(BaseModel):
    executor_id: UUID


class TaskCanceledMessage(BaseModel):
    task_id: UUID


class ExecutorHeartbeatMessage(BaseModel):
    timestamp: datetime
    executor_ids: list[UUID]


class TaskHeartbeatMessage(BaseModel):
    timestamp: datetime
    task_ids: list[UUID]
