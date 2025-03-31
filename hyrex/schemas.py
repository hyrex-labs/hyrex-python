from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


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
    parent_id: UUID | None
    task_name: str
    args: dict
    queue: str
    max_retries: int
    priority: int
    timeout_seconds: int | None
    idempotency_key: str | None
    status: TaskStatus
    workflow_run_id: UUID | None
    workflow_dependencies: list[UUID] | None


class DequeuedTask(BaseModel):
    id: UUID
    durable_id: UUID
    root_id: UUID
    parent_id: UUID | None
    task_name: str
    args: dict
    queue: str
    priority: int
    timeout_seconds: int | None
    scheduled_start: datetime | None
    queued: datetime
    started: datetime
    workflow_run_id: UUID | None
    attempt_number: int
    max_retries: int


class TaskResult(BaseModel):
    task_run_id: UUID
    created_at: datetime
    result: dict


# For tracking durable runs:
class TaskRun(BaseModel):
    id: UUID
    task_name: str
    max_retries: int
    attempt_number: int
    status: TaskStatus
    queued: datetime
    started: datetime | None
    finished: datetime | None
    task_result: TaskResult | None


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
    timeout_seconds: int | None
    idempotency_key: str | None


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
