import logging
import threading
import time
from queue import Empty, Queue
from uuid import UUID

import requests

from hyrex import constants
from hyrex.dispatcher.dispatcher import DequeuedTask, Dispatcher
from hyrex.models import HyrexTask, StatusEnum


class PlatformDispatcher(Dispatcher):

    HYREX_PLATFORM_URL = "https://platform-dev.hyrex.io"
    DEQUEUE_TASK_PATH = "/connect/dequeue-task"
    GET_STATUS_PATH = "/connect/get-task-status"
    UPDATE_STATUS_PATH = "/connect/update-task-status"
    ENQUEUE_TASK_PATH = "/connect/enqueue-task"

    def __init__(self, api_key: str, batch_size=100, flush_interval=0.1):
        self.api_key = api_key

        self.local_queue = Queue()
        self.running = True
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.thread = threading.Thread(target=self._batch_enqueue)
        self.thread.start()

    def enqueue(
        self,
        task: HyrexTask,
    ):
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

    # TODO: Add in all task fields once platform supports them
    def _enqueue_tasks(self, tasks: list[HyrexTask]):
        enqueue_url = f"{self.HYREX_PLATFORM_URL}{self.ENQUEUE_TASK_PATH}"
        headers = {
            "x-project-api-key": self.api_key,
        }
        data = {
            "tasks": [
                {
                    "id": str(task.id),
                    # "root_id": str(task.root_id),
                    "task_name": task.task_name,
                    "queue": task.queue,
                    "args": task.args,
                    "max_retries": task.max_retries,
                    # "priority": task.priority,
                }
                for task in tasks
            ]
        }
        try:
            response = requests.post(enqueue_url, headers=headers, json=data)
            if response.status_code != 200:
                logging.error(f"Error enqueuing task: {response.status_code}")
                logging.error(f"Response body: {response.text}")
        except requests.exceptions.RequestException as e:
            logging.error(f"Error enqueuing task via API: {str(e)}")
            raise RuntimeError(f"Failed to enqueue task via API: {e}")

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        logging.info("Stopping dispatcher...")
        self.running = False
        self.thread.join()
        logging.info("Dispatcher stopped successfully!")

    def dequeue(
        self,
        worker_id: UUID,
        queue: str = constants.DEFAULT_QUEUE,
        num_tasks: int = 1,
    ) -> list[DequeuedTask]:
        fetch_url = f"{self.HYREX_PLATFORM_URL}{self.DEQUEUE_TASK_PATH}"
        dequeued_tasks = []
        try:
            headers = {"x-project-api-key": self.api_key}
            json_body = {
                "queue": queue,
                "worker_id": str(worker_id),
                "num_tasks": num_tasks,
            }
            response = requests.post(fetch_url, headers=headers, json=json_body)

            if response.status_code == 200:
                data = response.json()
                if data["tasks"]:
                    task = data["tasks"][0]
                    dequeued_tasks.append(
                        DequeuedTask(
                            id=task["id"], name=task["task_name"], args=task["args"]
                        )
                    )
            else:
                logging.error(f"Error fetching task: {response.status_code}")
                error_body = response.text()  # Get the response body as text
                logging.error(f"Response body: {error_body}")

            return dequeued_tasks

        except Exception as e:
            logging.error(f"Exception while fetching task: {str(e)}")
            return None

    def _update_task_status(self, task_id: UUID, new_status: StatusEnum):
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
                logging.error(f"Error updating task status: {response.status_code}")

        except Exception as e:
            logging.error(f"Exception while updating task status: {str(e)}")

    def mark_success(self, task_id: UUID):
        self._update_task_status(task_id, StatusEnum.success)

    def mark_failed(self, task_id: UUID):
        self._update_task_status(task_id, StatusEnum.failed)

    # TODO: Update this once platform supports a full reset
    def reset_or_cancel_task(self, task_id: UUID):
        self._update_task_status(task_id, StatusEnum.queued)

    def attempt_retry(self, task_id: UUID):
        raise NotImplementedError("Retries not yet implemented on Hyrex platform")

    # TODO: Implement
    def cancel_task(self, task_id: UUID):
        pass

    def get_task_status(self, task_id: UUID) -> StatusEnum:
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
                logging.error(f"Error getting task status: {response.status_code}")
                logging.error(f"Response body: {response.text}")
        except Exception as e:
            if isinstance(e, ValueError):
                raise
            logging.error(f"Exception while getting task status: {str(e)}")

    def register_worker(self, worker_id: UUID):
        pass

    def mark_worker_stopped(self, worker_id: UUID):
        pass

    def get_workers_to_cancel(self, worker_ids: list[UUID]):
        pass
