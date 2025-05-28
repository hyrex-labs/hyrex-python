import sys
from datetime import datetime
from enum import Enum
from typing import Union, List
from uuid import UUID

# Python 3.9 compatibility - StrEnum was introduced in Python 3.11
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    class StrEnum(str, Enum):
        """Compatibility shim for Python < 3.11"""
        pass

from pydantic import BaseModel


class QueuePattern(BaseModel):
    glob_pattern: str
    postgres_pattern: str


class TaskStatus(StrEnum):
    success = "success"
    failed = "failed"
    up_for_cancel = "up_for_cancel"
    canceled = "canceled"
    running = "running"
    queued = "queued"
    waiting = "waiting"
    lost = "lost"
    skipped = "skipped"


class EnqueueTaskRequest(BaseModel):
    id: UUID
    durable_id: UUID
    root_id: UUID
    parent_id: Union[UUID, None]
    task_name: str
    args: dict
    queue: str
    max_retries: int
    priority: int
    timeout_seconds: Union[int, None]
    idempotency_key: Union[str, None]
    status: TaskStatus
    workflow_run_id: Union[UUID, None]
    workflow_dependencies: Union[List[UUID], None]


class DequeuedTask(BaseModel):
    id: UUID
    durable_id: UUID
    root_id: UUID
    parent_id: Union[UUID, None]
    task_name: str
    args: dict
    queue: str
    priority: int
    timeout_seconds: Union[int, None]
    scheduled_start: Union[datetime, None]
    queued: datetime
    started: datetime
    workflow_run_id: Union[UUID, None]
    attempt_number: int
    max_retries: int


# For tracking durable runs:
class TaskRun(BaseModel):
    id: UUID
    task_name: str
    max_retries: int
    attempt_number: int
    status: TaskStatus
    queued: datetime
    started: Union[datetime, None]
    finished: Union[datetime, None]
    result: Union[dict, None]


class WorkflowStatus(StrEnum):
    success = "success"
    failed = "failed"
    running = "running"
    queued = "queued"
    waiting = "waiting"
    up_for_cancel = "up_for_cancel"
    lost = "lost"
    canceled = "canceled"


class WorkflowRunRequest(BaseModel):
    id: UUID
    workflow_name: str
    args: dict
    queue: str
    timeout_seconds: Union[int, None]
    idempotency_key: Union[str, None]


class CronJob(BaseModel):
    jobid: int
    schedule: str
    command: str
    active: bool
    jobname: str
    activated_at: datetime
    scheduled_jobs_confirmed_until: datetime
    should_backfill: bool


class CronJobRun(BaseModel):
    jobid: int
    command: str
    schedule_time: datetime
