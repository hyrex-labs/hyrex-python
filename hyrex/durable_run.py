from datetime import datetime
import logging
import time
from uuid import UUID

from pydantic import BaseModel

from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.schemas import TaskRun, TaskStatus


class DurableTaskRun:
    def __init__(
        self,
        task_name: str,
        durable_id: UUID,
        dispatcher: Dispatcher,
    ):
        self.logger = logging.getLogger(__name__)
        self.task_name = task_name
        self.durable_id = durable_id
        self.dispatcher = dispatcher

        self.task_runs = list[TaskRun]

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
                if (
                    task.status == TaskStatus.failed
                    and task.attempt_number == task.max_retries
                ):
                    # Failed with no retries left
                    return False
            time.sleep(interval)
            elapsed = time.time() - start
            if elapsed > timeout:
                raise TimeoutError("Waiting for durable task run timed out.")

    def get_result(self):
        self.refresh()
        for task in self.task_runs:
            if task.status == TaskStatus.success and task.task_result is not None:
                # Return only the result dict, not the whole object
                return task.task_result.result
        self.logger.warning(f"No result found for durable run {self.durable_id}.")
        return None

    def cancel(self):
        # TODO: Find currently active task or try to mark all tasks as up for cancel.
        # self.dispatcher.try_to_cancel_task(self.task_run_id)
        raise NotImplementedError

    def __repr__(self):
        return f"DurableTaskRun<{self.task_name}>[{self.durable_id}]"

    def refresh(self):
        self.task_runs = self.dispatcher.get_durable_task_run_info(self.durable_id)
        for task in self.task_runs:
            pass
        # TODO: Compute any derived properties
