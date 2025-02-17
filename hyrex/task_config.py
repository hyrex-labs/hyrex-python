from pydantic import BaseModel, Field

from hyrex.hyrex_queue import HyrexQueue


class TaskConfig(BaseModel):
    queue: str | HyrexQueue | None = None
    priority: int | None = Field(default=None, ge=0, le=10)
    max_retries: int | None = Field(default=None, ge=0)
    timeout_seconds: int | None = Field(default=None, gt=0)
    idempotency_key: str | None = None

    def get_queue_name(self) -> str:
        if isinstance(self.queue, str):
            return self.queue
        else:
            return self.queue.name

    def merge(self, other: "TaskConfig") -> "TaskConfig":
        """Merge another config into this one, with other taking precedence"""
        if other is None:
            return self

        merged_data = self.model_dump()
        other_data = other.model_dump()

        for field, value in other_data.items():
            if value is not None:
                merged_data[field] = value

        return TaskConfig(**merged_data)
