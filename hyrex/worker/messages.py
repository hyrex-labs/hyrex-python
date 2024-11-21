from enum import StrEnum
from typing import Union
from uuid import UUID

from pydantic import BaseModel


class AdminMessageType(StrEnum):
    TERMINATED = "terminated"
    HEARTBEAT = "heartbeat"
    NEW_EXECUTOR = "new_executor"


class NewExecutor(BaseModel):
    executor_id: UUID


# Incoming messages to admin process
class AdminMessage(BaseModel):
    message_type: AdminMessageType
    # TODO: Update to payload
    task_ids: list[UUID]
    executor_ids: list[UUID]


class RootMessageType(StrEnum):
    HEARTBEAT_REQUEST = "heartbeat_request"
    CANCEL_TASK = "cancel_task"
    SET_EXECUTOR_TASK = "set_executor_task"


class CancelTask(BaseModel):
    task_id: UUID


class HeartbeatRequest(BaseModel):
    pass


class SetExecutorTask(BaseModel):
    executor_id: UUID
    task_id: UUID | None


# Incoming messages to root process
class RootMessage(BaseModel):
    message_type: RootMessageType
    payload: Union[CancelTask, SetExecutorTask, HeartbeatRequest]
