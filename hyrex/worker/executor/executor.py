import asyncio
import importlib
import json
import os
import random
import signal
import socket
import sys
import time
import traceback
from datetime import datetime, timezone
from inspect import signature
from multiprocessing import Event, Process, Queue
from pathlib import Path
from uuid import UUID

from psycopg.types.json import Json
from pydantic import BaseModel

from hyrex import constants
from hyrex.dispatcher import DequeuedTask, get_dispatcher
from hyrex.dispatcher.performance_dispatcher import PerformanceDispatcher
from hyrex.env_vars import EnvVars
from hyrex.hyrex_app import HyrexApp, HyrexAppInfo
from hyrex.hyrex_cache import HyrexCacheManager
from hyrex.hyrex_context import HyrexContext, clear_hyrex_context, set_hyrex_context
from hyrex.hyrex_queue import HyrexQueue
from hyrex.hyrex_registry import HyrexRegistry
from hyrex.schemas import QueuePattern
from hyrex.worker.executor.time_series_averager import TimeSeriesAverager
from hyrex.logging import get_logger, LogFeature
from hyrex.worker.messages.root_messages import (
    SetExecutorTaskMessage,
    TaskRegistrationComplete,
)
from hyrex.worker.s3_logs import write_task_logs_to_s3, write_task_logs_with_dispatcher
from hyrex.worker.utils import (
    glob_pattern_to_postgres_pattern,
    is_glob_pattern,
    is_process_alive,
)


def generate_executor_name():
    hostname = socket.gethostname()
    pid = os.getpid()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return f"hyrex-executor-{hostname}-{pid}-{timestamp}"


class HyrexTaskTimeout(Exception):
    pass


class WorkerExecutor(Process):

    def __init__(
        self,
        root_message_queue: Queue,
        log_level: str,
        app_module_path: str,
        executor_id: UUID,
        queue: str,
        executor_name: str,
        worker_name: str,
        register_app: bool = False,
    ):
        super().__init__()
        self.log_level = log_level

        self.root_message_queue = root_message_queue
        self._stop_event = Event()

        self.app_module_path = app_module_path
        self.queue = queue
        self.queue_pattern = None
        self.queues: list[HyrexQueue] = []
        self.executor_id = executor_id
        self.name = executor_name

        self.logs_s3_bucket = os.environ.get(EnvVars.LOGS_BUCKET)

        # Perf metrics
        self.num_distinct_queues_averager = TimeSeriesAverager()
        self.refresh_queue_duration_averager = TimeSeriesAverager()
        self.dequeue_duration_averager = TimeSeriesAverager()

        self.registry: HyrexRegistry = None
        self.register_app = register_app

        # To check if root process is running
        self.parent_pid = os.getpid()
        self.worker_name = worker_name

    def get_concurrency_for_queue(self, queue_name: str):
        return self.registry.get_concurrency_limit(queue_name=queue_name)

    def update_queue_list(self):
        self.queues = []
        self.logger.debug("Updating internal queue list from pattern...")

        start = time.perf_counter()
        queue_names = self.dispatcher.get_queues_for_pattern(self.queue_pattern)
        end = time.perf_counter()
        # Milliseconds
        self.refresh_queue_duration_averager.submit((end - start) * 1000)
        self.num_distinct_queues_averager.submit(len(queue_names))

        self.logger.debug(f"Queues found: {queue_names}")
        if queue_names:
            random.shuffle(queue_names)
        else:
            self._stop_event.wait(1.0)
            return

        for queue_name in queue_names:
            self.queues.append(
                HyrexQueue(
                    name=queue_name,
                    concurrency_limit=self.get_concurrency_for_queue(
                        queue_name=queue_name
                    ),
                )
            )

        self.dispatcher.update_executor_queues(self.executor_id, queue_names)

    def load_app_module(self):
        sys.path.append(str(Path.cwd()))
        module_path, instance_name = self.app_module_path.split(":")
        # Import the worker module
        app_module = importlib.import_module(module_path)
        app_instance: HyrexApp = getattr(app_module, instance_name)

        self.registry = app_instance.registry
        self.app_info = app_instance.app_info

    async def process_item(self, task: DequeuedTask):
        try:
            task_wrapper = self.registry.get_task(task.task_name)
        except KeyError:
            raise KeyError(f"Task '{task.task_name}' not found in Hyrex registry")
        result = await task_wrapper.async_call(**task.args)
        return result

    def fetch_task(self, queue: str, concurrency_limit: int = 0) -> DequeuedTask | None:
        start = time.perf_counter()
        dequeued_task = self.dispatcher.dequeue(
            executor_id=self.executor_id,
            # TODO: Cache this list.
            task_names=self.registry.get_task_names(),
            queue=queue,
            concurrency_limit=concurrency_limit,
        )
        end = time.perf_counter()
        # Milliseconds
        self.dequeue_duration_averager.submit((end - start) * 1000)
        return dequeued_task

    def mark_task_success(self, task_id: UUID, result: str):
        self.dispatcher.mark_success(task_id=task_id, result=result)

    def mark_task_failed(self, task_id: UUID):
        self.dispatcher.mark_failed(task_id=task_id)

    def retry_task(self, task_id: UUID, backoff_seconds: int):
        self.dispatcher.retry_task(task_id=task_id, backoff_seconds=backoff_seconds)

    # Notifies root process of current task being processed.
    def update_current_task(self, task_id: UUID):
        self.root_message_queue.put(
            SetExecutorTaskMessage(executor_id=self.executor_id, task_id=task_id),
        )

    def advance_workflow_if_needed(self, task: DequeuedTask):
        """Advance workflow if this task is part of one"""
        if task.workflow_run_id:
            self.logger.debug(
                "Advancing workflow",
                feature=LogFeature.WORKFLOW,
                workflow_run_id=str(task.workflow_run_id),
                task_id=str(task.id)
            )
            try:
                self.dispatcher.advance_workflow_run(
                    workflow_run_id=task.workflow_run_id
                )
            except Exception as workflow_error:
                self.logger.error(
                    "Failed to advance workflow",
                    feature=LogFeature.WORKFLOW,
                    workflow_run_id=str(task.workflow_run_id),
                    task_id=str(task.id),
                    error=str(workflow_error)
                )
                self.logger.debug(
                    f"Workflow advance error traceback:\n%s", traceback.format_exc()
                )

    async def process_task_with_logging(self, task: DequeuedTask):
        """Process a task with full logging context including error handling"""
        try:
            set_hyrex_context(
                HyrexContext(
                    task_id=task.id,
                    durable_id=task.durable_id,
                    root_id=task.root_id,
                    parent_id=task.parent_id,
                    task_name=task.task_name,
                    queue=task.queue,
                    priority=task.priority,
                    timeout_seconds=task.timeout_seconds,
                    scheduled_start=task.scheduled_start,
                    queued=task.queued,
                    started=task.started,
                    executor_id=self.executor_id,
                    attempt_number=task.attempt_number,
                    max_retries=task.max_retries,
                    workflow_run_id=task.workflow_run_id,
                )
            )

            # Notify root process of new task
            self.update_current_task(task.id)
            # Set up timeout
            if task.timeout_seconds:
                signal.alarm(task.timeout_seconds)
            # Run task
            result = await self.process_item(task)

            if result is not None:
                if isinstance(result, BaseModel):
                    result = result.model_dump_json()
                else:
                    try:
                        result = json.dumps(result)
                    except (TypeError, ValueError):
                        raise TypeError("Return value must be JSON-serializable")

            self.mark_task_success(task.id, result)

            self.logger.info(
                "Task completed successfully",
                feature=LogFeature.TASK_PROCESSING,
                task_id=str(task.id),
                task_name=task.task_name,
                queue=task.queue,
                attempt=task.attempt_number
            )

            # If this task is part of a workflow, advance it
            self.advance_workflow_if_needed(task)

        except Exception as e:
            self.logger.error(
                "Task processing failed",
                feature=LogFeature.TASK_PROCESSING,
                task_id=str(task.id),
                task_name=task.task_name,
                queue=task.queue,
                attempt=task.attempt_number,
                error_type=type(e).__name__,
                error_message=str(e)
            )
            self.logger.debug("Traceback:\n%s", traceback.format_exc())

            self.mark_task_failed(task.id)

            # Advance workflow even after task failure
            self.advance_workflow_if_needed(task)

            if task.attempt_number < task.max_retries:
                try:
                    backoff_seconds = self.registry.get_retry_backoff(
                        task_name=task.task_name, attempt_number=task.attempt_number
                    )
                    self.logger.info(
                        "Submitting task for retry",
                        feature=LogFeature.FLOW_CONTROL,
                        task_id=str(task.id),
                        task_name=task.task_name,
                        attempt=task.attempt_number,
                        max_retries=task.max_retries,
                        backoff_seconds=backoff_seconds
                    )
                    self.retry_task(
                        task_id=task.id, backoff_seconds=backoff_seconds
                    )
                except Exception as retry_error:
                    self.logger.error(f"Error during retry process: {retry_error}")
                    self.logger.error(
                        f"Retry error traceback:\n%s", traceback.format_exc()
                    )

            on_error = self.registry.get_on_error_handler(task.task_name)
            if on_error:
                self.logger.info(
                    f"Running on_error handler for task {task.task_name}"
                )
                try:
                    sig = signature(on_error)
                    if len(sig.parameters) == 0:
                        on_error()
                    else:
                        on_error(e)

                except Exception as on_error_exception:
                    self.logger.error(
                        "Exception hit when running on_error handler."
                    )
                    self.logger.error(on_error_exception)
                    self.logger.error("Traceback:\n%s", traceback.format_exc())

        finally:
            self.update_current_task(None)
            signal.alarm(0)  # Clear alarm
            clear_hyrex_context()

    def process(self, queue: HyrexQueue) -> bool:
        """Returns True if a task is found and attempted, False otherwise"""
        task: DequeuedTask | None = self.fetch_task(
            queue=queue.name, concurrency_limit=queue.concurrency_limit
        )
        if not task:
            return False

        try:
            # Run task with logging context based on dispatcher type
            if isinstance(self.dispatcher, PerformanceDispatcher):
                # Write logs via performance server
                async def run_with_logs():
                    async with write_task_logs_with_dispatcher(
                        task_id=task.id, dispatcher=self.dispatcher
                    ):
                        await self.process_task_with_logging(task)
                asyncio.run(run_with_logs())
            elif self.logs_s3_bucket:
                # Write logs directly to S3
                async def run_with_logs():
                    s3_log_link = None
                    try:
                        async with write_task_logs_to_s3(
                            task.id, self.logs_s3_bucket
                        ) as s3_log_link:
                            await self.process_task_with_logging(task)
                    finally:
                        if s3_log_link:
                            self.dispatcher.set_log_link(task.id, s3_log_link)
                asyncio.run(run_with_logs())
            else:
                # No logs written to S3
                asyncio.run(self.process_task_with_logging(task))

            self._stop_event.wait(0.5)  # Add delay after error
            return True
        finally:
            # 1/25 chance to publish stats
            if random.random() < 0.04:
                stats = {
                    "dequeueLatencyMs": [
                        avg.model_dump()
                        for avg in self.dequeue_duration_averager.get_time_series()
                    ],
                    "refreshQueueLatencyMs": [
                        avg.model_dump()
                        for avg in self.refresh_queue_duration_averager.get_time_series()
                    ],
                    "numDistinctQueues": [
                        avg.model_dump()
                        for avg in self.num_distinct_queues_averager.get_time_series()
                    ],
                }
                self.dispatcher.update_executor_stats(self.executor_id, stats)

            return True

    def check_root_process(self):
        # Confirm parent is still alive
        if not is_process_alive(self.parent_pid):
            self.logger.warning("Root process died unexpectedly. Shutting down.")
            self._stop_event.set()

    def run_static_queue_loop(self):
        queue = HyrexQueue(
            name=self.queue,
            concurrency_limit=self.registry.get_concurrency_limit(
                queue_name=self.queue
            ),
        )
        while not self._stop_event.is_set():
            # Queue pattern is a static string - fetch from it directly.
            if not self.process(queue):
                # No task found, sleep for a bit
                self._stop_event.wait(0.5)
            self.check_root_process()

    def run_round_robin_loop(self):
        self.update_queue_list()
        last_queue_refresh = time.monotonic()
        no_task_count = 0

        while not self._stop_event.is_set():
            seconds_since_queue_refresh = time.monotonic() - last_queue_refresh
            if (
                seconds_since_queue_refresh
                > constants.WORKER_EXECUTOR_QUEUE_REFRESH_SECONDS
                or len(self.queues) == 0
                or no_task_count >= 5
            ):
                self.update_queue_list()
                last_queue_refresh = time.monotonic()
                no_task_count = 0

            for queue in self.queues:
                self.check_root_process()
                if self._stop_event.is_set():
                    break

                if not self.process(queue=queue):
                    no_task_count += 1
                else:
                    no_task_count = 0

                # We're not hitting populated queues - pause and refresh queue list.
                if no_task_count >= 5:
                    break

    def register_hyrex_app(self):
        self.dispatcher.register_app(self.app_info.model_dump())

    def run(self):
        # Set log level in environment for TaskWrapper and other components
        os.environ["HYREX_LOG_LEVEL"] = self.log_level
        
        # Initialize logger in child process (multiprocessing requirement)
        self.logger = get_logger("executor", LogFeature.EXECUTOR, level=self.log_level)
        
        # Retrieve name and task registry from the provided app module path.
        self.load_app_module()

        # Convert queue pattern to Postgres SIMILAR TO pattern if needed.
        if is_glob_pattern(self.queue):
            self.queue_pattern = QueuePattern(
                glob_pattern=self.queue,
                postgres_pattern=glob_pattern_to_postgres_pattern(self.queue),
            )
            self.logger.debug(
                f"Converted queue glob to Postgres SIMILAR TO pattern: {self.queue_pattern.glob_pattern} -> {self.queue_pattern.postgres_pattern}"
            )

        try:
            self.dispatcher = get_dispatcher()
            self.dispatcher.register_executor(
                executor_id=self.executor_id,
                executor_name=self.name,
                queue_pattern=self.queue,
                queues=self.queues,
                worker_name=self.worker_name,
            )

            # Ignore termination signals, let main process manage shutdown.
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            signal.signal(signal.SIGINT, signal.SIG_IGN)

            if self.register_app:
                self.logger.info(
                    f"{self.name}: Registering app, tasks, and workflows to the DB."
                )
                self.registry.register_all_with_db()
                self.register_hyrex_app()
                # Notify root process that cron scheduler can start
                self.root_message_queue.put(TaskRegistrationComplete())

            # Set up to throw HyrexTaskTimeout and then end process on task timeout.
            def timeout_handler(signum, frame):
                self._stop_event.set()
                raise HyrexTaskTimeout()

            signal.signal(signal.SIGALRM, timeout_handler)

            self.logger.info(
                f"Executor process {self.name} started - checking for tasks."
            )

            # Run the main loop
            if self.queue_pattern:
                self.run_round_robin_loop()
            else:
                self.run_static_queue_loop()
        finally:
            self.stop()

    def stop(self):
        self.logger.info(f"Stopping {self.name}...")
        if self.dispatcher:
            self.dispatcher.disconnect_executor(self.executor_id)
            self.dispatcher.stop()
        # Clean up any cached resources
        HyrexCacheManager.cleanup()
        self.logger.info(f"{self.name} stopped successfully!")
