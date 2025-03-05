from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.dispatcher.dispatcher_provider import get_dispatcher


class HyrexContext(BaseModel):
    task_id: UUID
    durable_id: UUID
    root_id: UUID
    parent_id: UUID | None
    task_name: str
    queue: str
    priority: int
    timeout_seconds: int | None
    scheduled_start: datetime | None
    queued: datetime
    started: datetime
    executor_id: UUID
    attempt_number: int
    max_retries: int
    workflow_run_id: UUID | None

    workflow_run_args: dict | None = None

    def update_workflow_run_args(self):
        if self.workflow_run_id:
            dispatcher = get_dispatcher()
            self.workflow_run_args = dispatcher.get_workflow_run_args(
                self.workflow_run_id
            )


# Simple global context
_current_context: HyrexContext | None = None


def get_hyrex_context() -> HyrexContext | None:
    """Get the current Hyrex context."""
    return _current_context


def get_hyrex_workflow_args() -> dict | None:
    return _current_context.workflow_run_args


def set_hyrex_context(context: HyrexContext) -> None:
    """Set the current Hyrex context."""
    global _current_context
    _current_context = context
    if _current_context.workflow_run_id:
        _current_context.update_workflow_run_args()


def clear_hyrex_context() -> None:
    """Clear the current Hyrex context."""
    global _current_context
    _current_context = None
