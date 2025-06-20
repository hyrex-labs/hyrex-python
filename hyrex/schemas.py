from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel


class QueuePattern(BaseModel):
    glob_pattern: str
    postgres_pattern: str


class TaskStatus(StrEnum):
    success = "SUCCESS"
    failed = "FAILED"
    running = "RUNNING"
    queued = "QUEUED"
    up_for_cancel = "UP_FOR_CANCEL"
    canceled = "CANCELED"
    lost = "LOST"
    stopped = "STOPPED"
    skipped = "SKIPPED"
    await_deps = "AWAIT_DEPS"
    await_start_time = "AWAIT_START_TIME"


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
    result: dict | None


class WorkflowStatus(StrEnum):
    success = "SUCCESS"
    failed = "FAILED"
    running = "RUNNING"
    up_for_cancel = "UP_FOR_CANCEL"
    canceled = "CANCELED"
    asleep = "ASLEEP"


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
