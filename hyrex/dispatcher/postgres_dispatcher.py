import json
import random
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from typing import List, Type
from uuid import UUID

from psycopg import RawCursor
from psycopg.types.json import Json
from psycopg_pool import ConnectionPool
from pydantic import BaseModel
from uuid6 import uuid7

from hyrex import constants
from hyrex.dispatcher.dispatcher import Dispatcher
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
    WorkflowStatus,
)
from hyrex.sql import cron_sql, sql, workflow_sql


def pydantic_aware_default(obj):
    if isinstance(obj, BaseModel):
        # If the object is a Pydantic model, call model_dump()
        # to get its dictionary representation. json.dumps can handle dicts.
        return obj.model_dump()
    # If it's not a Pydantic model and json.dumps doesn't know it,
    # let the default TypeError happen.
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class PostgresDispatcher(Dispatcher):
    def __init__(self, conn_string: str, batch_size=1000, flush_interval=0.05):
        super().__init__()
        self.conn_string = conn_string
        self.pool = ConnectionPool(
            conn_string + "?keepalives=1&keepalives_idle=60&keepalives_interval=10",
            open=True,
            max_idle=5,
        )

        self.local_queue = Queue()
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.thread = threading.Thread(target=self._batch_enqueue, daemon=True)
        self.thread.start()
        self.stopping = False

        self.register_shutdown_handlers()

    @contextmanager
    def transaction(self):
        with self.pool.connection() as conn:
            with RawCursor(conn) as cur:
                try:
                    yield cur
                except InterruptedError:
                    conn.rollback()
                    raise
            conn.commit()

    def register_app(self, app_info: dict):
        with self.transaction() as cur:
            cur.execute(sql.REGISTER_APP_INFO_SQL, [1, json.dumps(app_info)])

    def mark_success(self, task_id: UUID, result: str):
        with self.transaction() as cur:
            cur.execute(sql.MARK_TASK_SUCCESS, [task_id])
            if result:
                cur.execute(sql.SAVE_RESULT, [task_id, result])

    def mark_failed(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.MARK_TASK_FAILED, [task_id])

    def set_log_link(self, task_id: UUID, log_link: str):
        with self.transaction() as cur:
            cur.execute(sql.SET_LOG_LINK, [task_id, log_link])

    def retry_task(self, task_id: UUID, backoff_seconds: int):
        if backoff_seconds > 0:
            scheduled_start = datetime.now(timezone.utc) + timedelta(
                seconds=backoff_seconds
            )
            with self.transaction() as cur:
                cur.execute(
                    sql.CREATE_RETRY_TASK_WITH_BACKOFF,
                    [task_id, uuid7(), scheduled_start],
                )
        else:
            with self.transaction() as cur:
                cur.execute(
                    sql.CREATE_RETRY_TASK,
                    [task_id, uuid7()],
                )

    def try_to_cancel_task(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.TRY_TO_CANCEL_TASK, [task_id])

    def task_canceled(self, task_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.TASK_CANCELED, [task_id])

    def dequeue(
        self,
        executor_id: UUID,
        task_names: list[str],
        queue: str = constants.ANY_QUEUE,
        concurrency_limit: int = 0,
    ) -> DequeuedTask:
        dequeued_task = None
        with self.transaction() as cur:
            if concurrency_limit > 0:
                cur.execute(
                    sql.FETCH_TASK_WITH_CONCURRENCY,
                    [queue, concurrency_limit, executor_id, task_names],
                )
            else:
                cur.execute(sql.FETCH_TASK, [queue, executor_id, task_names])
            row = cur.fetchone()
            if row:
                (
                    task_id,
                    durable_id,
                    root_id,
                    parent_id,
                    task_name,
                    args,
                    queue,
                    priority,
                    timeout_seconds,
                    scheduled_start,
                    queued,
                    started,
                    workflow_run_id,
                    attempt_number,
                    max_retries,
                ) = row
                dequeued_task = DequeuedTask(
                    id=task_id,
                    durable_id=durable_id,
                    root_id=root_id,
                    parent_id=parent_id,
                    task_name=task_name,
                    args=args,
                    queue=queue,
                    priority=priority,
                    timeout_seconds=timeout_seconds,
                    scheduled_start=scheduled_start,
                    queued=queued,
                    started=started,
                    workflow_run_id=workflow_run_id,
                    attempt_number=attempt_number,
                    max_retries=max_retries,
                )

        return dequeued_task

    def enqueue(self, tasks: list[EnqueueTaskRequest]):
        if not tasks:
            self.logger.error("Task list is empty - cannot enqueue.")
            return
        if self.stopping:
            self.logger.warning("Task enqueued during shutdown. May not be processed.")
        for task in tasks:
            self.local_queue.put(task)

    def _batch_enqueue(self):
        tasks = []
        last_flush_time = time.monotonic()

        while True:
            # Use a longer timeout and bulk collect tasks
            try:
                # Collect as many tasks as possible with one lock acquisition
                task = self.local_queue.get(timeout=self.flush_interval)
                if task is None:  # Stop signal
                    break

                tasks.append(task)
                # Drain queue without blocking (significantly reduces lock contention)
                while len(tasks) < self.batch_size:
                    try:
                        task = self.local_queue.get_nowait()
                        if task is None:
                            break
                        tasks.append(task)
                    except Empty:
                        break

                # Check if we should flush based on time or batch size
                current_time = time.monotonic()
                if (current_time - last_flush_time >= self.flush_interval) or len(
                    tasks
                ) >= self.batch_size:
                    if tasks:
                        self._enqueue_tasks(tasks)
                        tasks = []
                    last_flush_time = current_time

            except Empty:
                # Flush on timeout if we have tasks
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()

    def _enqueue_tasks(self, tasks: List[EnqueueTaskRequest]):
        """
        Inserts a batch of tasks into the database.

        :param tasks: List of tasks to insert.
        """
        task_data = []
        for task in tasks:
            # Convert args to JSON, handling Pydantic models
            try:
                # json.dumps will handle dicts, lists, strings, numbers etc. directly.
                # If it encounters a Pydantic model (either as task.args itself or nested),
                # it will call our pydantic_aware_default function.
                args_json = json.dumps(task.args, default=pydantic_aware_default)
            except TypeError as e:
                self.logger.error(
                    f"Task {task.id}: Failed to serialize args to JSON: {e}"
                )
                raise

            task_data.append(
                (
                    task.id,
                    task.durable_id,
                    task.root_id,
                    task.parent_id,
                    task.task_name,
                    args_json,  # Already a JSON string
                    task.queue,
                    task.max_retries,
                    task.priority,
                    task.timeout_seconds,
                    task.idempotency_key,
                    task.status,
                    task.workflow_run_id,
                    task.workflow_dependencies,
                )
            )

        with self.transaction() as cur:
            cur.executemany(
                sql.ENQUEUE_TASK,
                task_data,
            )

    def stop(self, timeout: float = 5.0) -> bool:
        # Check if already stopping/stopped
        if self.stopping:
            return True

        self.logger.debug("Stopping dispatcher...")
        self.stopping = True

        # Signal the batch thread to stop and wait with timeout
        self.local_queue.put(None)
        self.thread.join(timeout=timeout)

        clean_shutdown = not self.thread.is_alive()

        # Close the connection pool
        if clean_shutdown:
            self.pool.close()
        else:
            self.logger.warning(
                "Batch thread did not stop cleanly, forcing connection pool to close"
            )
            self.pool.close(timeout=1.0)

        self.logger.debug(
            "Dispatcher stopped %s.",
            "successfully" if clean_shutdown else "with timeout",
        )
        return clean_shutdown

    def get_task_status(self, task_id: UUID) -> TaskStatus:
        with self.transaction() as cur:
            cur.execute(sql.GET_TASK_STATUS, [task_id])
            result = cur.fetchone()
            if result is None:
                raise ValueError(f"Task id {task_id} not found in DB.")
            return result[0]

    def register_executor(
        self,
        executor_id: UUID,
        executor_name: str,
        queue_pattern: str,
        queues: list[HyrexQueue],
        worker_name: str,
    ):
        with self.transaction() as cur:
            cur.execute(
                sql.REGISTER_EXECUTOR,
                [executor_id, executor_name, queue_pattern, queues, worker_name],
            )

    def disconnect_executor(self, executor_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.DISCONNECT_EXECUTOR, [executor_id])

    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        with self.transaction() as cur:
            cur.execute(sql.EXECUTOR_HEARTBEAT, [timestamp, executor_ids])

    def update_executor_stats(self, executor_id: UUID, stats: dict):
        with self.transaction() as cur:
            cur.execute(sql.UPDATE_EXECUTOR_STATS, [executor_id, Json(stats)])

    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        with self.transaction() as cur:
            cur.execute(sql.TASK_HEARTBEAT, [timestamp, task_ids])

    def get_tasks_up_for_cancel(self) -> list[UUID]:
        with self.transaction() as cur:
            cur.execute(sql.GET_TASKS_UP_FOR_CANCEL)
            return [row[0] for row in cur.fetchall()]

    def mark_running_tasks_lost(self, executor_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.MARK_RUNNING_TASKS_LOST, [executor_id])

    def save_result(self, task_id: UUID, result: str):
        with self.transaction() as cur:
            cur.execute(sql.SAVE_RESULT, [task_id, result])

    # TODO: Handle durable runs, unfinished tasks.
    def get_result(self, task_id: UUID) -> dict:
        with self.transaction() as cur:
            cur.execute(sql.FETCH_RESULT, [task_id])
            row = cur.fetchone()

            if row is None:
                return None

            result = row[0]
            return result

    def get_queues_for_pattern(self, pattern: QueuePattern) -> list[str]:
        with self.transaction() as cur:
            cur.execute(sql.GET_QUEUES_FOR_PATTERN, [pattern.postgres_pattern])
            return [row[0] for row in cur.fetchall()]

    def register_task_def(
        self,
        task_name: str,
        arg_schema: dict,
        default_config: dict,
        cron: str = None,
        source_code: str = None,
    ):
        with self.transaction() as cur:
            cur.execute(
                sql.UPSERT_TASK,
                [
                    task_name,
                    Json(arg_schema),
                    Json(default_config),
                    cron,
                    source_code,
                ],
            )

            cron_job_name = f"ScheduledTask-{task_name}"
            if cron:
                current_id = uuid7()
                task_request = EnqueueTaskRequest(
                    id=current_id,
                    durable_id=current_id,
                    workflow_run_id=None,
                    workflow_dependencies=None,
                    root_id=current_id,
                    parent_id=None,
                    queue="TODO",
                    status=TaskStatus.queued,
                    task_name=task_name,
                    args={},
                    max_retries=0,  # TODO
                    priority=1,  # TODO
                    timeout_seconds=None,  # TODO
                    idempotency_key=None,  # TODO
                )
                insert_task_command = cron_sql.create_insert_task_cron_expression(
                    task_request
                )
                cur.execute(
                    cron_sql.CREATE_CRON_JOB_FOR_TASK,
                    [cron, insert_task_command, cron_job_name],
                )
            else:
                cur.execute(cron_sql.TURN_OFF_CRON_FOR_TASK, [cron_job_name])

    def register_workflow(
        self,
        name: str,
        source_code: str,
        workflow_dag_json: dict,
        workflow_arg_schema: Type[BaseModel] | None,
        default_config: dict,
    ):
        with self.transaction() as cur:
            cron = None
            cur.execute(
                workflow_sql.UPSERT_WORKFLOW,
                [
                    name,
                    cron,
                    source_code,
                    Json(workflow_dag_json),
                    (
                        Json(workflow_arg_schema.model_json_schema())
                        if workflow_arg_schema
                        else None
                    ),
                    Json(default_config),
                ],
            )

    def send_workflow_run(self, workflow_run_request: WorkflowRunRequest) -> UUID:
        with self.transaction() as cur:
            cur.execute(
                workflow_sql.INSERT_WORKFLOW_RUN,
                [
                    workflow_run_request.id,
                    workflow_run_request.workflow_name,
                    Json(workflow_run_request.args),
                    workflow_run_request.queue,
                    workflow_run_request.timeout_seconds,
                    workflow_run_request.idempotency_key,
                ],
            )
            result = cur.fetchall()
            if len(result) != 1:
                raise ValueError(f"Insert workflow run failed.")
            return result[0][0]

    def advance_workflow_run(self, workflow_run_id: UUID):
        self.logger.info(f"Advancing workflow run {workflow_run_id}")

        with self.transaction() as cur:
            # First query to check status
            cur.execute(
                workflow_sql.SET_WORKFLOW_RUN_STATUS_BASED_ON_TASK_RUNS,
                [workflow_run_id],
            )
            result = cur.fetchall()

            if len(result) != 1:
                self.logger.warning(
                    "Result of SET_WORKFLOW_RUN_STATUS_BASED_ON_TASK_RUNS is not one row."
                )
                return None

            workflow_status = result[0][1]  # Status is second column
            if workflow_status in (WorkflowStatus.failed, WorkflowStatus.success):
                return None

            # Second query to advance the workflow
            cur.execute(workflow_sql.ADVANCE_WORKFLOW_RUN, [workflow_run_id])
            return None

    def get_workflow_run_args(self, workflow_run_id: UUID) -> dict:
        with self.transaction() as cur:
            cur.execute(workflow_sql.GET_WORKFLOW_RUN_ARGS, [workflow_run_id])
            result = cur.fetchone()
            return result[0] if result else None

    def acquire_scheduler_lock(self, worker_name: str) -> int | None:
        lock_duration = "2 minutes"
        with self.transaction() as cur:
            cur.execute(cron_sql.ACQUIRE_SCHEDULER_LOCK, [worker_name, lock_duration])
            result = cur.fetchone()
            return result[0] if result else None

    def pull_cron_job_expressions(self) -> list[CronJob]:
        with self.transaction() as cur:
            cur.execute(cron_sql.PULL_ACTIVE_CRON_EXPRESSIONS)
            rows = cur.fetchall()
            return [
                CronJob(
                    jobid=row[0],
                    schedule=row[1],
                    command=row[2],
                    active=row[3],
                    jobname=row[4],
                    activated_at=row[5],
                    scheduled_jobs_confirmed_until=row[6],
                    should_backfill=row[7],
                )
                for row in rows
            ]

    def update_cron_job_confirmation_timestamp(self, jobid: int):
        with self.transaction() as cur:
            cur.execute(cron_sql.UPDATE_CRON_JOB_CONFIRMATION_TS, [jobid])

    def schedule_cron_job_runs(self, cron_job_runs: List[CronJobRun]) -> None:
        if not cron_job_runs:
            return

        # Check all jobs have same ID
        all_same_id = all(job.jobid == cron_job_runs[0].jobid for job in cron_job_runs)
        if not all_same_id:
            job_ids = [job.jobid for job in cron_job_runs]
            self.logger.error(f"Got jobIds {job_ids}, {cron_job_runs}")
            raise ValueError(
                "All cronJobsRuns submitted here need to have the same job id."
            )

        # Execute the SQL
        with self.transaction() as cur:
            sql_dict = cron_sql.cron_job_runs_to_sql(cron_job_runs)
            self.logger.debug(
                f"cron-scheduling: <====== Running SQL =======>:\n\n{sql_dict['sql']}\n\n{sql_dict['values']}\n\n<==== DONE =====>\n\n"
            )
            cur.execute(sql_dict["sql"], sql_dict["values"])

        # Update confirmation timestamp
        self.update_cron_job_confirmation_timestamp(cron_job_runs[0].jobid)

    def execute_queued_cron_job_run(self) -> str | None:
        with self.transaction() as cur:
            cur.execute("SELECT execute_queued_command();")
            rows = cur.fetchall()
            if not rows:
                raise ValueError("Hyrex framework error.")
            return rows[0][0]  # "executed" or "not_found"

    def register_cron_sql_query(
        self,
        cron_job_name: str,
        cron_sql_query: str,
        cron_expr: str,
        should_backfill: bool,
    ) -> None:
        """Register a new cron job for executing a SQL query on a schedule."""
        with self.transaction() as cur:
            cur.execute(
                cron_sql.CREATE_CRON_JOB_FOR_SQL_QUERY,
                [cron_expr, cron_sql_query, cron_job_name, should_backfill],
            )

    def release_scheduler_lock(self, worker_name: str) -> None:
        """Release the scheduler lock for the specified worker."""
        with self.transaction() as cur:
            cur.execute(cron_sql.RELEASE_SCHEDULER_LOCK, [worker_name])

    def get_durable_run_tasks(self, durable_id: UUID) -> list[TaskRun]:
        with self.transaction() as cur:
            cur.execute(sql.GET_TASK_RUNS_BY_DURABLE_ID, [durable_id])
            results = cur.fetchall()

            task_runs = []
            for row in results:
                (
                    task_id,
                    task_name,
                    max_retries,
                    attempt_number,
                    status,
                    queued,
                    started,
                    finished,
                    task_result,
                ) = row

                # Create the TaskRun object without the result field
                task_run = TaskRun(
                    id=task_id,
                    task_name=task_name,
                    max_retries=max_retries,
                    attempt_number=attempt_number,
                    status=status,
                    queued=queued,
                    started=started,
                    finished=finished,
                    result=task_result if task_result is not None else {},
                )
                task_runs.append(task_run)

            return task_runs

    def get_workflow_durable_runs(self, workflow_run_id: UUID) -> list[UUID]:
        with self.transaction() as cur:
            cur.execute(sql.GET_WORKFLOW_DURABLE_RUNS, [workflow_run_id])
            results = cur.fetchall()
            return [row[0] for row in results]

    def try_to_cancel_durable_run(self, durable_id: UUID):
        with self.transaction() as cur:
            cur.execute(sql.TRY_TO_CANCEL_DURABLE_RUN, [durable_id])

    def update_executor_queues(self, executor_id: UUID, queues: list[str]):
        with self.transaction() as cur:
            cur.execute(sql.UPDATE_EXECUTOR_QUEUES, [executor_id, queues])
