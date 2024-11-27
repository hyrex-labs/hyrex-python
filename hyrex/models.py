from datetime import datetime, timezone
from enum import StrEnum
from typing import Optional
from uuid import UUID

import psycopg
from sqlalchemy import JSON, CheckConstraint, Index, Integer, desc
from sqlmodel import Column, DateTime, Field, Relationship, SQLModel
from uuid_extensions import uuid7

from hyrex import constants, sql


def utcnow():
    return datetime.now(timezone.utc)


class StatusEnum(StrEnum):
    success = "success"
    failed = "failed"
    up_for_cancel = "up_for_cancel"
    canceled = "canceled"
    running = "running"
    queued = "queued"
    waiting = "waiting"
    lost = "lost"


class HyrexExecutor(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid7, primary_key=True)
    name: str

    queue: str

    started: datetime | None = Field(
        sa_column=DateTime(timezone=True), default_factory=utcnow
    )
    last_heartbeat: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )
    stopped: datetime | None = Field(sa_column=DateTime(timezone=True), default=None)


class HyrexTask(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid7, primary_key=True)
    root_id: UUID
    parent_id: UUID | None

    # These 4 are indexed
    task_name: str = Field(index=True)
    status: StatusEnum = Field(default=StatusEnum.queued, index=True)
    queue: str = Field(default=constants.DEFAULT_QUEUE, index=True)
    scheduled_start: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True), index=True), default=None
    )

    executor_id: UUID | None

    queued: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default_factory=utcnow
    )
    started: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )
    last_heartbeat: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )
    finished: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )

    max_retries: int = 0
    attempt_number: int = 0

    # Define the priority field with a constraint between 1 and 10
    priority: int = Field(
        sa_column=Column(Integer, CheckConstraint("priority BETWEEN 1 AND 10")),
        default=constants.DEFAULT_PRIORITY,
    )

    args: dict = Field(default_factory=dict, sa_column=Column(JSON))

    result: Optional["HyrexTaskResult"] = Relationship(back_populates="task")

    __table_args__ = (
        Index("index_queue_status", "status", "queue", "scheduled_start", "task_name"),
        Index(
            "idx_hyrextask_queue_status_priority",
            "queue",
            "status",
            desc("priority"),
        ),
    )


class HyrexTaskResult(SQLModel, table=True):
    id: int = Field(primary_key=True)

    task_id: UUID | None = Field(foreign_key="hyrextask.id")
    task: HyrexTask | None = Relationship(back_populates="result")

    result: dict = Field(default_factory=dict, sa_column=Column(JSON))


def create_tables(conn_string):
    with psycopg.connect(conn_string) as conn:
        with conn.cursor() as cur:
            cur.execute(sql.CREATE_HYREX_TASK_TABLE)
            cur.execute(sql.CREATE_HYREX_RESULT_TABLE)
            cur.execute(sql.CREATE_HYREX_EXECUTOR_TABLE)
        conn.commit()
