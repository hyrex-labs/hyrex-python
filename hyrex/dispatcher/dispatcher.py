import atexit
import logging
import signal
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Type
from uuid import UUID

from pydantic import BaseModel

from hyrex import constants
from hyrex.configs import TaskConfig
from hyrex.hyrex_queue import HyrexQueue
from hyrex.schemas import (
    CronJob,
    CronJobRun,
    DequeuedTask,
    EnqueueTaskRequest,
    QueuePattern,
    TaskRun,
    TaskStatus,
    WorkflowRunRequest,
)


class Dispatcher(ABC):
    logger = logging.getLogger(__name__)

    def _signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        self.logger.debug(f"\nReceived {signame}. Shutting down Hyrex dispatcher...")
        self.stop()

    def _chain_signal_handlers(self, new_handler, old_handler):
        """Return a function that calls both the new and old signal handlers."""

        def wrapper(signum, frame):
            # Call the new handler first
            new_handler(signum, frame)
            # Then call the previous handler (if it exists)
            if old_handler and callable(old_handler):
                old_handler(signum, frame)

        return wrapper

    def _setup_signal_handlers(self):
        for sig in (signal.SIGTERM, signal.SIGINT):
            old_handler = signal.getsignal(sig)  # Get the existing handler
            new_handler = self._signal_handler  # Your new handler
            # Set the new handler, which calls both new and old handlers
            signal.signal(sig, self._chain_signal_handlers(new_handler, old_handler))

    def register_shutdown_handlers(self):
        self._setup_signal_handlers()
        atexit.register(self.stop)

    def __init__(self):
        pass

    @abstractmethod
    def register_app(self, app_info: dict):
        pass

    @abstractmethod
    def enqueue(
        self,
        tasks: list[EnqueueTaskRequest],
    ):
        pass

    @abstractmethod
    def dequeue(
        self,
        executor_id: UUID,
        task_names: list[str],
        queue: str = constants.ANY_QUEUE,
        concurrency_limit: int = 0,
    ) -> DequeuedTask:
        pass

    # Result must be a JSON string)
    @abstractmethod
    def mark_success(self, task_id: UUID, result: str):
        pass

    @abstractmethod
    def mark_failed(self, task_id: UUID):
        pass

    @abstractmethod
    def set_log_link(self, task_id: UUID, log_link: str):
        pass

    @abstractmethod
    def retry_task(self, task_id: UUID, backoff_seconds: int):
        pass

    @abstractmethod
    def try_to_cancel_task(self, task_id: UUID):
        pass

    @abstractmethod
    def try_to_cancel_durable_run(self, durable_id: UUID):
        pass

    @abstractmethod
    def task_canceled(self, task_id: UUID):
        pass

    # TODO: Remove?
    @abstractmethod
    def get_result(self, task_id: UUID) -> dict:
        pass

    @abstractmethod
    def get_task_status(self, task_id: UUID) -> TaskStatus:
        pass

    @abstractmethod
    def register_task_def(
        self,
        task_name: str,
        arg_schema: Type[BaseModel] | None,
        task_config: TaskConfig,
        cron: str = None,
        source_code: str = None,
    ):
        pass

    @abstractmethod
    def register_executor(
        self,
        executor_id: UUID,
        executor_name: str,
        queue_pattern: str,
        queues: list[HyrexQueue],
        worker_name: str,
    ):
        pass

    @abstractmethod
    def disconnect_executor(self, executor_id: UUID):
        pass

    @abstractmethod
    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        pass

    @abstractmethod
    def update_executor_stats(self, executor_id: UUID, stats: dict):
        pass

    @abstractmethod
    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        pass

    @abstractmethod
    def get_tasks_up_for_cancel(self) -> list[UUID]:
        pass

    @abstractmethod
    def get_queues_for_pattern(self, pattern: QueuePattern) -> list[str]:
        pass

    @abstractmethod
    def acquire_scheduler_lock(self, worker_name: str) -> int | None:
        pass

    @abstractmethod
    def pull_cron_job_expressions(self) -> list[CronJob]:
        pass

    @abstractmethod
    def update_cron_job_confirmation_timestamp(self, jobid: UUID):
        pass

    @abstractmethod
    def schedule_cron_job_runs(self, cron_job_runs: list[CronJobRun]):
        pass

    @abstractmethod
    def execute_queued_cron_job_run(self) -> str | None:
        pass

    @abstractmethod
    def release_scheduler_lock(self, worker_name: str) -> None:
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def register_workflow(
        self,
        name: str,
        source_code: str,
        workflow_dag_json: str,
        workflow_arg_schema: Type[BaseModel] | None,
        default_config: dict,
        cron: str | None,
    ):
        pass

    @abstractmethod
    def send_workflow_run(self, workflow_run_request: WorkflowRunRequest) -> UUID:
        pass

    @abstractmethod
    def advance_workflow_run(self, workflow_run_id: UUID):
        pass

    @abstractmethod
    def get_workflow_run_args(self, workflow_run_id: UUID) -> dict:
        pass

    @abstractmethod
    def register_cron_sql_query(
        self,
        cron_job_name: str,
        cron_sql_query: str,
        cron_expr: str,
        should_backfill: bool,
    ) -> None:
        pass

    @abstractmethod
    def get_durable_run_tasks(self, durable_id: UUID) -> list[TaskRun]:
        pass

    @abstractmethod
    def get_workflow_durable_runs(self, workflow_run_id: UUID) -> list[UUID]:
        pass

    @abstractmethod
    def update_executor_queues(self, executor_id: UUID, queues: list[str]):
        pass
