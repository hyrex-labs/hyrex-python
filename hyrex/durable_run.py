import logging
from uuid import UUID

from pydantic import BaseModel

from hyrex.dispatcher.dispatcher import Dispatcher


class TaskRun(BaseModel):
    def __init__(
        self,
        task_name: str,
        task_run_id: str,
        dispatcher: Dispatcher,
    ):

        self.task_name = task_name
        self.task_run_id = task_run_id
        self.dispatcher = dispatcher

    def wait(self, timeout: float = 30.0, interval: float = 1.0):
        start = time.time()
        elapsed = 0
        try:
            task_status = self.dispatcher.get_task_status(task_id=self.task_run_id)
        except ValueError:
            # Task hasn't yet moved from self.local_queue to DB
            task_status = TaskStatus.queued

        while task_status in [TaskStatus.queued, TaskStatus.running]:
            if elapsed > timeout:
                raise TimeoutError("Waiting for task timed out.")
            time.sleep(interval)
            task_status = self.dispatcher.get_task_status(task_id=self.task_run_id)
            elapsed = time.time() - start

    # TODO: Implement
    def get_result(self):
        return self.dispatcher.get_result(self.task_run_id)

    def cancel(self):
        self.dispatcher.try_to_cancel_task(self.task_run_id)

    def __repr__(self):
        return f"TaskRun<{self.task_name}>[{self.task_run_id}]"


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

        self.task_runs = []

    def refresh(self):
        # TODO
        pass

    def wait(self):
        # TODO
        pass

    def get_result(self):
        # TODO
        pass
