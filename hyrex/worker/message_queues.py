from enum import StrEnum

from pydantic import BaseModel


class AdminMessageType(StrEnum):
    TERMINATED = "terminated"
    HEARTBEAT = "heartbeat"
    NEW_EXECUTOR = "new_executor"


class MessageToAdmin(BaseModel):
    type: AdminMessageType
    task_ids: list[str]
    executor_ids: list[str]


class WorkerMessageType(StrEnum):
    HEARTBEAT_REQUEST = "heartbeat_request"
    TERMINATION_REQUEST = "termination_request"
    EXECUTOR_TASK_UPDATE = "executor_task_update"


class MessageToWorker(BaseModel):
    type: WorkerMessageType
    task_id: str
    executor_id: str
