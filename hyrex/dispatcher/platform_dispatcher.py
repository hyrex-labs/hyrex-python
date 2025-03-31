import threading
import time
from datetime import datetime
from queue import Empty, Queue
from typing import Type
from uuid import UUID

import requests
from pydantic import BaseModel

from hyrex import constants
from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.hyrex_queue import HyrexQueue
from hyrex.schemas import (CronJob, CronJobRun, DequeuedTask,
                           EnqueueTaskRequest, TaskRun, TaskStatus,
                           WorkflowRunRequest)


class PlatformDispatcher(Dispatcher):

    # TODO: Update with correct URL and paths
    # HYREX_PLATFORM_URL = "https://platform-dev.hyrex.io"
    HYREX_PLATFORM_URL = "http://localhost:8000"
    ENQUEUE_TASK_PATH = "/enqueue"
    DEQUEUE_TASK_PATH = "/connect/dequeue-task"
    GET_STATUS_PATH = "/connect/get-task-status"
    UPDATE_STATUS_PATH = "/connect/update-task-status"

    def __init__(self, api_key: str, batch_size=100, flush_interval=0.1):
        super().__init__()
        self.api_key = api_key

        self.local_queue = Queue()
        self.running = True
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.thread = threading.Thread(target=self._batch_enqueue, daemon=True)
        self.thread.start()

        self.register_shutdown_handlers()

    def register_app(self, app_info: dict):
        pass

    def enqueue(
        self,
        tasks: list[EnqueueTaskRequest],
    ):
        for task in tasks:
            self.local_queue.put(task)

    def _batch_enqueue(self):
        tasks = []
        last_flush_time = time.monotonic()
        while self.running or not self.local_queue.empty():
            timeout = self.flush_interval - (time.monotonic() - last_flush_time)
            if timeout <= 0:
                # Flush if the flush interval has passed
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()
                continue

            try:
                # Wait for a task or until the timeout expires
                task = self.local_queue.get(timeout=timeout)
                tasks.append(task)
                if len(tasks) >= self.batch_size:
                    # Flush if batch size is reached
                    self._enqueue_tasks(tasks)
                    tasks = []
                    last_flush_time = time.monotonic()
            except Empty:
                # No task received within the timeout
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()

        # Flush any remaining tasks when stopping
        if tasks:
            self._enqueue_tasks(tasks)

    def _enqueue_tasks(self, tasks: list[EnqueueTaskRequest]):
        enqueue_url = f"{self.HYREX_PLATFORM_URL}{self.ENQUEUE_TASK_PATH}"
        headers = {
            "X-API-Key": self.api_key,
        }

        task_list_json = [task.model_dump(mode="json") for task in tasks]

        try:
            response = requests.post(enqueue_url, headers=headers, json=task_list_json)
            if response.status_code != 200:
                self.logger.error(f"Error enqueuing task: {response.status_code}")
                self.logger.error(f"Response body: {response.text}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error enqueuing task via API: {str(e)}")
            raise RuntimeError(f"Failed to enqueue task via API: {e}")

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        self.logger.debug("Stopping dispatcher...")
        self.running = False
        self.thread.join()
        self.logger.debug("Dispatcher stopped successfully!")

    def dequeue(
        self,
        executor_id: UUID,
        task_names: list[str],
        queue: str = constants.ANY_QUEUE,
        concurrency_limit: int = 0,
    ) -> DequeuedTask:
        # TODO: Apply concurrency limit
        fetch_url = f"{self.HYREX_PLATFORM_URL}{self.DEQUEUE_TASK_PATH}"
        try:
            headers = {"x-project-api-key": self.api_key}
            dequeued_task = None
            json_body = {
                "queue": queue,
                "worker_id": str(worker_id),
                "num_tasks": 1,
            }
            response = requests.post(fetch_url, headers=headers, json=json_body)

            if response.status_code == 200:
                data = response.json()
                if data["tasks"]:
                    task = data["tasks"][0]
                    dequeued_task = DequeuedTask(
                        id=task["id"], name=task["task_name"], args=task["args"]
                    )

            else:
                self.logger.error(f"Error fetching task: {response.status_code}")
                error_body = response.text()  # Get the response body as text
                self.logger.error(f"Response body: {error_body}")

            return dequeued_task

        except Exception as e:
            self.logger.error(f"Exception while fetching task: {str(e)}")
            return None

    def _update_task_status(self, task_id: UUID, new_status: TaskStatus):
        update_task_url = f"{self.HYREX_PLATFORM_URL}{self.UPDATE_STATUS_PATH}"
        try:
            headers = {"x-project-api-key": self.api_key}
            data = {
                "task_updates": [
                    {"task_id": str(task_id), "updated_status": new_status}
                ]
            }
            response = requests.post(update_task_url, headers=headers, json=data)

            if response.status_code != 200:
                self.logger.error(f"Error updating task status: {response.status_code}")

        except Exception as e:
            self.logger.error(f"Exception while updating task status: {str(e)}")

    def mark_success(self, task_id: UUID):
        self._update_task_status(task_id, TaskStatus.success)

    def mark_failed(self, task_id: UUID):
        self._update_task_status(task_id, TaskStatus.failed)

    def retry_task(self, task_id: UUID, backoff_seconds: int):
        raise NotImplementedError("Retries not yet implemented on Hyrex platform")

    # TODO: Implement
    def try_to_cancel_task(self, task_id: UUID):
        raise NotImplementedError("Cancellation not yet implemented on Hyrex platform")

    def task_canceled(self, task_id: UUID):
        raise NotImplementedError("Cancellation not yet implemented on Hyrex platform")

    def get_task_status(self, task_id: UUID) -> TaskStatus:
        get_status_url = f"{self.HYREX_PLATFORM_URL}{self.GET_STATUS_PATH}"
        try:
            headers = {"x-project-api-key": self.api_key}
            data = {"task_ids": [str(task_id)]}
            response = requests.get(get_status_url, headers=headers, json=data)

            if response.status_code == 200:
                response_data = response.json()
                if response_data.get("result"):
                    return response_data.get("result")[0].get("status")
                else:
                    raise ValueError(f"Task status not returned for task ID {task_id}")
            else:
                self.logger.error(f"Error getting task status: {response.status_code}")
                self.logger.error(f"Response body: {response.text}")
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            self.logger.error(f"Exception while getting task status: {str(e)}")

    def register_executor(
        self,
        executor_id: UUID,
        executor_name: str,
        queue_pattern: str,
        queues: list[HyrexQueue],
        worker_name: str,
    ):
        pass

    def disconnect_executor(self, executor_id: UUID):
        pass

    def mark_running_tasks_lost(self, executor_id: UUID):
        pass

    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        pass

    def update_executor_stats(self, executor_id: UUID, stats: dict):
        pass

    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        pass

    def get_tasks_up_for_cancel(self) -> list[UUID]:
        pass

    def get_queues_for_pattern(self, pattern: str) -> list[str]:
        pass

    def register_task(
        self,
        task_name: str,
        arg_schema: Type[BaseModel] | None,
        default_config: dict,
        cron: str = None,
        source_code: str = None,
    ):
        pass

    def acquire_scheduler_lock(self, worker_name: str) -> int | None:
        pass

    def pull_cron_job_expressions(self) -> list[CronJob]:
        pass

    def update_cron_job_confirmation_timestamp(self, jobid: int):
        pass

    def schedule_cron_job_runs(self, cron_job_runs: list[CronJobRun]):
        pass

    def register_cron_sql_query(
        self,
        cron_job_name: str,
        cron_sql_query: str,
        cron_expr: str,
        should_backfill: bool,
    ) -> None:
        pass

    def release_scheduler_lock(self, worker_name: str) -> None:
        pass

    def register_workflow(
        self,
        name: str,
        source_code: str,
        workflow_dag_json: str,
        workflow_arg_schema: Type[BaseModel] | None,
        default_config: dict,
    ):
        pass

    def send_workflow_run(self, workflow_run_request: WorkflowRunRequest) -> UUID:
        pass

    def advance_workflow_run(self, workflow_run_id: UUID):
        pass

    def get_workflow_run_args(self, workflow_run_id: UUID) -> dict:
        pass

    def get_durable_run_tasks(self, durable_id: UUID) -> list[TaskRun]:
        pass
        
    def get_workflow_durable_runs(self, workflow_run_id: UUID) -> list[UUID]:
        pass

    def try_to_cancel_durable_run(self, durable_id: UUID):
        pass

    def update_executor_queues(self, executor_id: UUID, queues: list[str]):
        pass

    def save_result(self, task_id: UUID, result: str):
        pass

    def get_result(self, task_id: UUID) -> dict:
        pass
