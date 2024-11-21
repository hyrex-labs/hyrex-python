from uuid import UUID

from pydantic import BaseModel


class NewExecutorMessage(BaseModel):
    executor_id: UUID


class ExecutorStoppedMessage(BaseModel):
    executor_id: UUID


class TaskCanceledMessage(BaseModel):
    task_id: UUID


class ExecutorHeartbeatMessage(BaseModel):
    executor_ids: list[UUID]


class TaskHeartbeatMessage(BaseModel):
    task_ids: list[UUID]
