import logging
import time
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, PrivateAttr

from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.dispatcher.dispatcher_provider import get_dispatcher
from hyrex.schemas import TaskRun, TaskStatus


class DurableTaskRun(BaseModel):
    task_name: str
    durable_id: UUID
    task_runs: list[TaskRun] = []

    _dispatcher: Dispatcher = PrivateAttr(default_factory=get_dispatcher)
    _logger: logging.Logger = PrivateAttr(
        default_factory=lambda: logging.getLogger(__name__)
    )

    def wait(self, timeout: float = 30.0, interval: float = 0.5) -> bool:
        start = time.time()
        elapsed = 0

        run_complete = False

        while not run_complete:
            self.refresh()
            for task in self.task_runs:
                if task.status == TaskStatus.success:
                    # Completed successfully
                    return True
                elif (
                    task.status == TaskStatus.failed
                    and task.attempt_number == task.max_retries
                ):
                    # Failed with no retries left
                    return False
                elif task.status == TaskStatus.canceled:
                    # Canceled
                    return False

            time.sleep(interval)
            elapsed = time.time() - start
            if elapsed > timeout:
                raise TimeoutError("Waiting for durable task run timed out.")

    def get_result(self):
        self.refresh()
        for task in self.task_runs:
            if task.status == TaskStatus.success and task.result is not None:
                return task.result
        self._logger.warning(f"No result found for durable run {self.durable_id}.")
        return None

    def cancel(self):
        self._dispatcher.try_to_cancel_durable_run(self.durable_id)

    def __repr__(self):
        return f"DurableTaskRun<{self.task_name}>[{self.durable_id}]"

    def refresh(self):
        self.task_runs = self._dispatcher.get_durable_run_tasks(self.durable_id)

        # Update task_name from the first task run
        if self.task_runs and len(self.task_runs) > 0:
            self.task_name = self.task_runs[0].task_name
