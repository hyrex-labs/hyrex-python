from enum import StrEnum
from typing import Union

from pydantic import BaseModel


class AdminMessageType(StrEnum):
    TERMINATED = "terminated"
    HEARTBEAT = "heartbeat"
    NEW_EXECUTOR = "new_executor"


# Incoming messages to admin process
class AdminMessage(BaseModel):
    type: AdminMessageType
    task_ids: list[str]
    executor_ids: list[str]


class WorkerMessageType(StrEnum):
    HEARTBEAT_REQUEST = "heartbeat_request"
    CANCEL_TASK = "cancel_task"
    SET_EXECUTOR_TASK = "set_executor_task"


class CancelTask(BaseModel):
    task_id: str


class HeartbeatRequest(BaseModel):
    pass


class SetExecutorTask(BaseModel):
    executor_id: str
    task_id: str


# Incoming messages to main worker process
class WorkerMessage(BaseModel):
    type: WorkerMessageType
    message: Union[CancelTask, SetExecutorTask, HeartbeatRequest]
    task_id: str
    executor_id: str
