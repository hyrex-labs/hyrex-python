from sqlmodel import Field, SQLModel, Relationship, Column, create_engine, DateTime
from sqlalchemy import Index
from enum import StrEnum
from datetime import datetime
from sqlalchemy import JSON
from uuid import UUID
from uuid_extensions import uuid7


class StatusEnum(StrEnum):
    success = "success"
    failed = "failed"
    up_for_retry = "up_for_retry"
    running = "running"
    queued = "queued"


class HyrexWorker(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid7, primary_key=True)
    name: str

    started: datetime | None = Field(sa_column=DateTime(timezone=True), default=None)
    finished: datetime | None = Field(sa_column=DateTime(timezone=True), default=None)


class HyrexTask(SQLModel, table=True):
    id: UUID | None = Field(default_factory=uuid7, primary_key=True)

    # These 4 are indexed
    task_name: str = Field(index=True)
    status: StatusEnum = Field(default=StatusEnum.queued, index=True)
    queue: str = Field(default="default", index=True)
    scheduled_start: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True), index=True), default=None
    )

    worker_id: UUID | None

    created: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )
    started: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )
    finished: datetime | None = Field(
        sa_column=Column(DateTime(timezone=True)), default=None
    )
    retried: int = 0

    args: dict = Field(default_factory=dict, sa_column=Column(JSON))

    results: list["HyrexTaskResult"] = Relationship(back_populates="task")

    __table_args__ = (
        Index("index_queue_status", "status", "queue", "scheduled_start", "task_name"),
    )


class HyrexTaskResult(SQLModel, table=True):
    id: int = Field(primary_key=True)

    task_id: UUID | None = Field(foreign_key="hyrextask.id")
    task: HyrexTask | None = Relationship(back_populates="results")

    results: dict = Field(default_factory=dict, sa_column=Column(JSON))


def create_tables(conn_string):
    engine = create_engine(conn_string)
    SQLModel.metadata.create_all(engine)
