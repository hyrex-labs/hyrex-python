from datetime import datetime
from uuid import UUID

from pydantic import BaseModel

from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.dispatcher.dispatcher_provider import get_dispatcher
from hyrex.durable_run import DurableTaskRun


class HyrexWorkflowContext(BaseModel):
    workflow_run_id: UUID
    workflow_args: dict | None = None
    durable_runs: dict[str, DurableTaskRun] = {}


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

    workflow_context: HyrexWorkflowContext | None = None

    def update_workflow_context(self):
        if self.workflow_run_id:
            dispatcher = get_dispatcher()

            # Initialize workflow context if it doesn't exist
            if not self.workflow_context:
                self.workflow_context = HyrexWorkflowContext(
                    workflow_run_id=self.workflow_run_id
                )

            # Get workflow args
            self.workflow_context.workflow_args = dispatcher.get_workflow_run_args(
                self.workflow_run_id
            )

            # Get workflow durable runs
            durable_ids = dispatcher.get_workflow_durable_runs(self.workflow_run_id)

            # Create dictionary of DurableTaskRun objects indexed by task name
            self.workflow_context.durable_runs = {}

            for durable_id in durable_ids:
                # Create a DurableTaskRun for each durable ID
                durable_run = DurableTaskRun(
                    task_name="",  # Will be updated after refresh
                    durable_id=durable_id,
                )

                # Refresh to get the task runs from database
                durable_run.refresh()

                # Add to the dictionary using task name as the key if we have a task name
                if durable_run.task_name:
                    # TODO: Check for clobbering
                    self.workflow_context.durable_runs[durable_run.task_name] = (
                        durable_run
                    )


# Simple global context
_current_context: HyrexContext | None = None


def get_hyrex_context() -> HyrexContext | None:
    """Get the current Hyrex context."""
    return _current_context


def get_hyrex_workflow_context() -> HyrexWorkflowContext | None:
    """Get the workflow context for the current task execution."""
    if _current_context:
        return _current_context.workflow_context
    return None


def set_hyrex_context(context: HyrexContext) -> None:
    """Set the current Hyrex context."""
    global _current_context
    _current_context = context
    if _current_context.workflow_run_id:
        _current_context.update_workflow_context()


def clear_hyrex_context() -> None:
    """Clear the current Hyrex context."""
    global _current_context
    _current_context = None
