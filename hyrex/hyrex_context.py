from datetime import datetime

from pydantic import BaseModel


# TODO: Finalize context fields.
class HyrexContext(BaseModel):
    task_id: str
    root_id: str
    task_name: str
    queue: str
    priority: int
    scheduled_start: str | None
    queued: datetime
    started: datetime
    executor_id: str


# Simple global context
_current_context: HyrexContext | None = None


def get_hyrex_context() -> HyrexContext | None:
    """Get the current Hyrex context."""
    return _current_context


def set_hyrex_context(context: HyrexContext) -> None:
    """Set the current Hyrex context."""
    global _current_context
    _current_context = context


def clear_hyrex_context() -> None:
    """Clear the current Hyrex context."""
    global _current_context
    _current_context = None
