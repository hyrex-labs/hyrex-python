import json
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from queue import Empty, Queue
from typing import List, Type
from uuid import UUID

import sqlalchemy
from pydantic import BaseModel
from sqlalchemy import create_engine
from uuid6 import uuid7

from hyrex import constants
from hyrex.configs import TaskConfig
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

# Import SQLC generated models and parameter classes
from .sqlc import models
from .sqlc import (
    # Import modules containing parameter classes
    register_app_info,
    transition_task_state,
    save_result,
    set_log_link,
    conditionally_retry_task,
    fetch_task,
    fetch_task_with_concurrency_limit,
    create_task_run,
    get_task_run_by_id,
    register_executor,
    disconnect_executor,
    batch_update_heartbeat_on_executors,
    update_executor_stats,
    batch_update_heartbeat_log,
    fetch_active_queue_names,
    register_task_def,
    create_cron_job_for_task,
    turn_off_cron_for_task,
    register_workflow,
    create_workflow_run,
    set_workflow_run_status_based_on_task_runs,
    advance_workflow_run_func,
    get_workflow_run_by_id,
    acquire_scheduler_lock,
    pull_active_cron_expressions,
    update_cron_job_confirmation_ts,
    schedule_cron_job_runs_json,
    trigger_execute_queued_cron_job,
    create_cron_job_for_sql_query,
    release_scheduler_lock,
    get_task_attempts_by_durable_id,
    get_workflow_run_task_runs,
    update_queues_on_executor,
    fetch_result,
    # Import sync functions
    register_app_info_sync,
    transition_task_state_sync,
    save_result_sync,
    set_log_link_sync,
    conditionally_retry_task_sync,
    fetch_task_sync,
    fetch_task_with_concurrency_limit_sync,
    create_task_run_sync,
    get_task_run_by_id_sync,
    register_executor_sync,
    disconnect_executor_sync,
    batch_update_heartbeat_on_executors_sync,
    update_executor_stats_sync,
    batch_update_heartbeat_log_sync,
    fetch_active_queue_names_sync,
    register_task_def_sync,
    create_cron_job_for_task_sync,
    turn_off_cron_for_task_sync,
    register_workflow_sync,
    create_workflow_run_sync,
    set_workflow_run_status_based_on_task_runs_sync,
    advance_workflow_run_func_sync,
    get_workflow_run_by_id_sync,
    acquire_scheduler_lock_sync,
    pull_active_cron_expressions_sync,
    update_cron_job_confirmation_ts_sync,
    schedule_cron_job_runs_json_sync,
    trigger_execute_queued_cron_job_sync,
    create_cron_job_for_sql_query_sync,
    release_scheduler_lock_sync,
    get_task_attempts_by_durable_id_sync,
    get_workflow_run_task_runs_sync,
    update_queues_on_executor_sync,
    fetch_result_sync,
)


def pydantic_aware_default(obj):
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class SqlcDispatcher(Dispatcher):
    """
    A dispatcher implementation that uses SQLC-generated queries with SQLAlchemy.
    """

    def __init__(self, conn_string: str, batch_size=1000, flush_interval=0.05):
        super().__init__()
        self.conn_string = conn_string

        # Create SQLAlchemy engine with psycopg3
        # Replace postgresql:// with postgresql+psycopg:// to use psycopg3
        if conn_string.startswith("postgresql://"):
            conn_string = conn_string.replace(
                "postgresql://", "postgresql+psycopg://", 1
            )
        elif conn_string.startswith("postgres://"):
            conn_string = conn_string.replace("postgres://", "postgresql+psycopg://", 1)

        self.engine = create_engine(
            conn_string,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 60,
                "keepalives_interval": 10,
            },
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
        """Context manager for database transactions."""
        with self.engine.begin() as conn:
            yield conn

    def register_app(self, app_info: dict):
        with self.transaction() as conn:
            register_app_info_sync(
                conn,
                register_app_info.RegisterAppInfoParams(
                    id=1, app_info=json.dumps(app_info)
                ),
            )

    def mark_success(self, task_id: UUID, result: str):
        with self.transaction() as conn:
            # Transition task state to success
            transition_task_state_sync(
                conn,
                transition_task_state.TransitionTaskStateParams(
                    task_id=task_id, next_state="SUCCESS"
                ),
            )
            # Save result if provided
            if result:
                save_result_sync(
                    conn, save_result.SaveResultParams(task_id=task_id, result=result)
                )

    def mark_failed(self, task_id: UUID):
        with self.transaction() as conn:
            transition_task_state_sync(
                conn,
                transition_task_state.TransitionTaskStateParams(
                    task_id=task_id, next_state="FAILED"
                ),
            )

    def set_log_link(self, task_id: UUID, log_link: str):
        with self.transaction() as conn:
            set_log_link_sync(
                conn, set_log_link.SetLogLinkParams(task_id=task_id, log_link=log_link)
            )

    def retry_task(self, task_id: UUID, backoff_seconds: int):
        scheduled_start = None
        if backoff_seconds > 0:
            scheduled_start = datetime.now(timezone.utc) + timedelta(
                seconds=backoff_seconds
            )

        with self.transaction() as conn:
            conditionally_retry_task_sync(
                conn,
                conditionally_retry_task.ConditionallyRetryTaskParams(
                    task_id=task_id,
                    new_task_id=uuid7(),
                    scheduled_start=scheduled_start,
                ),
            )

    def try_to_cancel_task(self, task_id: UUID):
        with self.transaction() as conn:
            transition_task_state_sync(
                conn,
                transition_task_state.TransitionTaskStateParams(
                    task_id=task_id, next_state="UP_FOR_CANCEL"
                ),
            )

    def task_canceled(self, task_id: UUID):
        with self.transaction() as conn:
            transition_task_state_sync(
                conn,
                transition_task_state.TransitionTaskStateParams(
                    task_id=task_id, next_state="CANCELED"
                ),
            )

    def dequeue(
        self,
        executor_id: UUID,
        task_names: list[str],
        queue: str = constants.ANY_QUEUE,
        concurrency_limit: int = 0,
    ) -> DequeuedTask:
        dequeued_task = None
        with self.transaction() as conn:
            if concurrency_limit > 0:
                row = fetch_task_with_concurrency_limit_sync(
                    conn,
                    fetch_task_with_concurrency_limit.FetchTaskWithConcurrencyLimitParams(
                        queue=queue,
                        concurrency_limit=concurrency_limit,
                        executor_id=executor_id,
                        task_names=task_names,
                    ),
                )
            else:
                row = fetch_task_sync(
                    conn,
                    fetch_task.FetchTaskParams(
                        executor_id=executor_id, queue=queue, task_names=task_names
                    ),
                )

            if row:
                dequeued_task = DequeuedTask(
                    id=row.id,
                    durable_id=row.durable_id,
                    root_id=row.root_id,
                    parent_id=row.parent_id,
                    task_name=row.task_name,
                    args=row.args,
                    queue=row.queue,
                    priority=row.priority,
                    timeout_seconds=row.timeout_seconds,
                    scheduled_start=row.scheduled_start,
                    queued=row.queued,
                    started=row.started,
                    workflow_run_id=row.workflow_run_id,
                    attempt_number=row.attempt_number,
                    max_retries=row.max_retries,
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
            try:
                task = self.local_queue.get(timeout=self.flush_interval)
                if task is None:  # Stop signal
                    break

                tasks.append(task)
                # Drain queue without blocking
                while len(tasks) < self.batch_size:
                    try:
                        task = self.local_queue.get_nowait()
                        if task is None:
                            break
                        tasks.append(task)
                    except Empty:
                        break

                # Check if we should flush
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
        """Inserts a batch of tasks into the database using SQLC."""
        with self.transaction() as conn:
            for task in tasks:
                # Convert args to JSON, handling Pydantic models
                try:
                    args_json = json.dumps(task.args, default=pydantic_aware_default)
                except TypeError as e:
                    self.logger.error(
                        f"Task {task.id}: Failed to serialize args to JSON: {e}"
                    )
                    raise

                # Create task using SQLC generated function
                create_task_run_sync(
                    conn,
                    create_task_run.CreateTaskRunParams(
                        id=task.id,
                        durable_id=task.durable_id,
                        root_id=task.root_id,
                        parent_id=task.parent_id,
                        status=task.status.value,  # Convert enum to string
                        task_name=task.task_name,
                        args=args_json,  # Pass JSON string directly
                        queue=task.queue,
                        max_retries=task.max_retries,
                        priority=task.priority,
                        timeout_seconds=task.timeout_seconds,
                        idempotency_key=task.idempotency_key,
                        scheduled_start=datetime.now(
                            timezone.utc
                        ),  # Use current time for scheduled_start
                        workflow_run_id=task.workflow_run_id,
                        workflow_dependencies=task.workflow_dependencies or [],
                    ),
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

        # Close the engine
        if clean_shutdown:
            self.engine.dispose()
        else:
            self.logger.warning(
                "Batch thread did not stop cleanly, forcing engine to close"
            )
            self.engine.dispose()

        self.logger.debug(
            "Dispatcher stopped %s.",
            "successfully" if clean_shutdown else "with timeout",
        )
        return clean_shutdown

    def get_task_status(self, task_id: UUID) -> TaskStatus:
        with self.transaction() as conn:
            task_run = get_task_run_by_id_sync(
                conn, get_task_run_by_id.GetTaskRunByIdParams(id=task_id)
            )
            if task_run is None:
                raise ValueError(f"Task id {task_id} not found in DB.")
            return TaskStatus(task_run.status)

    def register_executor(
        self,
        executor_id: UUID,
        executor_name: str,
        queue_pattern: str,
        queues: list[HyrexQueue],
        worker_name: str,
    ):
        with self.transaction() as conn:
            register_executor_sync(
                conn,
                register_executor.RegisterExecutorParams(
                    id=executor_id,
                    name=executor_name,
                    queue_pattern=queue_pattern,
                    queues=[q.name for q in queues],
                    worker_name=worker_name,
                ),
            )

    def disconnect_executor(self, executor_id: UUID):
        with self.transaction() as conn:
            disconnect_executor_sync(
                conn,
                disconnect_executor.DisconnectExecutorParams(
                    stats=None, id=executor_id  # No stats on disconnect
                ),
            )

    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        with self.transaction() as conn:
            batch_update_heartbeat_on_executors_sync(
                conn,
                batch_update_heartbeat_on_executors.BatchUpdateHeartbeatOnExecutorsParams(
                    executor_ids=executor_ids
                ),
            )

    def update_executor_stats(self, executor_id: UUID, stats: dict):
        with self.transaction() as conn:
            update_executor_stats_sync(
                conn,
                update_executor_stats.UpdateExecutorStatsParams(
                    id=executor_id, stats=json.dumps(stats)
                ),
            )

    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        # TODO: Implement task heartbeat functionality
        # The current batch_update_heartbeat_log is for executor heartbeats, not tasks
        pass

    def get_tasks_up_for_cancel(self) -> list[UUID]:
        # This needs to be implemented based on business logic
        # For now, return empty list
        return []

    def get_queues_for_pattern(self, pattern: QueuePattern) -> list[str]:
        with self.transaction() as conn:
            return list(
                fetch_active_queue_names_sync(
                    conn,
                    fetch_active_queue_names.FetchActiveQueueNamesParams(
                        queue_pattern=pattern.postgres_pattern
                    ),
                )
            )

    def register_task_def(
        self,
        task_name: str,
        arg_schema: Type[BaseModel] | None,
        task_config: TaskConfig,
        cron: str = None,
        source_code: str = None,
    ):
        with self.transaction() as conn:
            # Convert arg_schema to dict if it's a Pydantic model class
            arg_schema_dict = None
            if arg_schema and hasattr(arg_schema, "model_json_schema"):
                arg_schema_dict = json.dumps(arg_schema.model_json_schema())

            register_task_def_sync(
                conn,
                register_task_def.RegisterTaskDefParams(
                    task_name=task_name,
                    cron_expr=cron,
                    source_code=source_code,
                    arg_schema=arg_schema_dict,
                    queue=(
                        task_config.get_queue_name() if task_config.queue else "default"
                    ),
                    priority=(
                        task_config.priority if task_config.priority is not None else 5
                    ),
                    max_retries=(
                        task_config.max_retries
                        if task_config.max_retries is not None
                        else 0
                    ),
                    timeout_seconds=task_config.timeout_seconds,
                ),
            )

            # Handle cron job creation/deletion
            cron_job_name = f"ScheduledTask-{task_name}"
            if cron:
                # Create cron job for task
                current_id = uuid7()
                task_request = EnqueueTaskRequest(
                    id=current_id,
                    durable_id=current_id,
                    workflow_run_id=None,
                    workflow_dependencies=None,
                    root_id=current_id,
                    parent_id=None,
                    queue=(
                        task_config.get_queue_name() if task_config.queue else "default"
                    ),
                    status=TaskStatus.queued,
                    task_name=task_name,
                    args={},
                    max_retries=(
                        task_config.max_retries
                        if task_config.max_retries is not None
                        else 0
                    ),
                    priority=(
                        task_config.priority if task_config.priority is not None else 5
                    ),
                    timeout_seconds=task_config.timeout_seconds,
                    idempotency_key=None,
                )
                # Convert task request to SQL command string
                insert_task_command = self._create_insert_task_command(task_request)
                create_cron_job_for_task_sync(
                    conn,
                    create_cron_job_for_task.CreateCronJobForTaskParams(
                        schedule=cron,
                        command=insert_task_command,
                        jobname=cron_job_name,
                    ),
                )
            else:
                turn_off_cron_for_task_sync(
                    conn,
                    turn_off_cron_for_task.TurnOffCronForTaskParams(
                        jobname=cron_job_name
                    ),
                )

    def _create_insert_task_command(self, task: EnqueueTaskRequest) -> str:
        """Create SQL command for inserting a task (used by cron jobs)."""
        # This is a simplified version - you may need to adjust based on your exact SQL
        return f"""
        INSERT INTO hyrex_task_run (
            id, durable_id, root_id, parent_id, task_name, args, queue,
            max_retries, priority, timeout_seconds, idempotency_key, status,
            workflow_run_id, workflow_dependencies, attempt_number, queued
        ) VALUES (
            '{task.id}'::uuid, '{task.durable_id}'::uuid, '{task.root_id}'::uuid,
            {f"'{task.parent_id}'::uuid" if task.parent_id else 'NULL'},
            '{task.task_name}', '{json.dumps(task.args)}'::json, '{task.queue}',
            {task.max_retries}, {task.priority},
            {task.timeout_seconds if task.timeout_seconds else 'NULL'},
            {f"'{task.idempotency_key}'" if task.idempotency_key else 'NULL'},
            '{task.status.value}'::task_run_status,
            {f"'{task.workflow_run_id}'::uuid" if task.workflow_run_id else 'NULL'},
            {f"ARRAY{task.workflow_dependencies}::uuid[]" if task.workflow_dependencies else 'NULL'},
            0, CURRENT_TIMESTAMP
        )
        """

    def register_workflow(
        self,
        name: str,
        source_code: str,
        workflow_dag_json: dict,
        workflow_arg_schema: Type[BaseModel] | None,
        default_config: dict,
        cron: str | None,
    ):
        with self.transaction() as conn:
            register_workflow_sync(
                conn,
                register_workflow.RegisterWorkflowParams(
                    workflow_name=name,
                    cron_expr=cron,
                    source_code=source_code,
                    dag_structure=json.dumps(workflow_dag_json),
                ),
            )

    def send_workflow_run(self, workflow_run_request: WorkflowRunRequest) -> UUID:
        with self.transaction() as conn:
            workflow_run_id = create_workflow_run_sync(
                conn,
                create_workflow_run.CreateWorkflowRunParams(
                    workflow_run_id=workflow_run_request.id,
                    workflow_name=workflow_run_request.workflow_name,
                    args=json.dumps(
                        workflow_run_request.args, default=pydantic_aware_default
                    ),
                    queue=workflow_run_request.queue,
                    timeout_seconds=workflow_run_request.timeout_seconds,
                    idempotency_key=workflow_run_request.idempotency_key,
                ),
            )
            if workflow_run_id is None:
                raise ValueError("Insert workflow run failed.")
            return workflow_run_id

    def advance_workflow_run(self, workflow_run_id: UUID):
        self.logger.info(f"Advancing workflow run {workflow_run_id}")

        with self.transaction() as conn:
            # First update workflow run status based on task runs
            updated_row = set_workflow_run_status_based_on_task_runs_sync(
                conn,
                set_workflow_run_status_based_on_task_runs.SetWorkflowRunStatusBasedOnTaskRunsParams(
                    workflow_run_id=workflow_run_id
                ),
            )

            if updated_row and updated_row.status in ("FAILED", "SUCCESS"):
                return None

            # Advance the workflow
            list(
                advance_workflow_run_func_sync(
                    conn,
                    advance_workflow_run_func.AdvanceWorkflowRunFuncParams(
                        workflow_run_id=workflow_run_id
                    ),
                )
            )
            return None

    def get_workflow_run_args(self, workflow_run_id: UUID) -> dict:
        with self.transaction() as conn:
            workflow_run = get_workflow_run_by_id_sync(
                conn,
                get_workflow_run_by_id.GetWorkflowRunByIdParams(id=workflow_run_id),
            )
            return workflow_run.args if workflow_run else None

    def acquire_scheduler_lock(self, worker_name: str) -> int | None:
        with self.transaction() as conn:
            lock_id = acquire_scheduler_lock_sync(
                conn,
                acquire_scheduler_lock.AcquireSchedulerLockParams(
                    worker_name=worker_name, duration=timedelta(minutes=2)
                ),
            )
            return lock_id

    def pull_cron_job_expressions(self) -> list[CronJob]:
        with self.transaction() as conn:
            rows = list(
                pull_active_cron_expressions_sync(
                    conn, pull_active_cron_expressions.PullActiveCronExpressionsParams()
                )
            )
            return [
                CronJob(
                    jobid=row.jobid,
                    schedule=row.schedule,
                    command=row.command,
                    active=row.active,
                    jobname=row.jobname,
                    activated_at=row.activated_at,
                    scheduled_jobs_confirmed_until=row.scheduled_jobs_confirmed_until,
                    should_backfill=row.should_backfill,
                )
                for row in rows
            ]

    def update_cron_job_confirmation_timestamp(self, jobid: int):
        with self.transaction() as conn:
            update_cron_job_confirmation_ts_sync(
                conn,
                update_cron_job_confirmation_ts.UpdateCronJobConfirmationTsParams(
                    jobid=jobid
                ),
            )

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
        with self.transaction() as conn:
            # Convert cron job runs to JSON format expected by SQLC
            cron_jobs_json = [
                {
                    "jobid": job.jobid,
                    "command": job.command,
                    "schedule_time": job.schedule_time.isoformat(),
                }
                for job in cron_job_runs
            ]

            schedule_cron_job_runs_json_sync(
                conn,
                schedule_cron_job_runs_json.ScheduleCronJobRunsJsonParams(
                    runs_json=json.dumps(cron_jobs_json)
                ),
            )

        # Update confirmation timestamp
        self.update_cron_job_confirmation_timestamp(cron_job_runs[0].jobid)

    def execute_queued_cron_job_run(self) -> str | None:
        with self.transaction() as conn:
            result = trigger_execute_queued_cron_job_sync(
                conn,
                trigger_execute_queued_cron_job.TriggerExecuteQueuedCronJobParams(),
            )
            return result  # "executed" or "not_found"

    def register_cron_sql_query(
        self,
        cron_job_name: str,
        cron_sql_query: str,
        cron_expr: str,
        should_backfill: bool,
    ) -> None:
        with self.transaction() as conn:
            create_cron_job_for_sql_query_sync(
                conn,
                create_cron_job_for_sql_query.CreateCronJobForSqlQueryParams(
                    schedule=cron_expr,
                    command=cron_sql_query,
                    jobname=cron_job_name,
                    should_backfill=should_backfill,
                ),
            )

    def release_scheduler_lock(self, worker_name: str) -> None:
        with self.transaction() as conn:
            release_scheduler_lock_sync(
                conn,
                release_scheduler_lock.ReleaseSchedulerLockParams(
                    worker_name=worker_name
                ),
            )

    def get_durable_run_tasks(self, durable_id: UUID) -> list[TaskRun]:
        with self.transaction() as conn:
            results = list(
                get_task_attempts_by_durable_id_sync(
                    conn,
                    get_task_attempts_by_durable_id.GetTaskAttemptsByDurableIdParams(
                        durable_id=durable_id
                    ),
                )
            )

            task_runs = []
            for row in results:
                # Fetch result if exists
                result_data = {}
                if row.finished and row.status == "SUCCESS":
                    result_row = fetch_result_sync(
                        conn, fetch_result.FetchResultParams(task_id=row.id)
                    )
                    if result_row:
                        result_data = result_row.result

                task_run = TaskRun(
                    id=row.id,
                    task_name=row.task_name,
                    max_retries=row.max_retries,
                    attempt_number=row.attempt_number,
                    status=row.status,
                    queued=row.queued,
                    started=row.started,
                    finished=row.finished,
                    result=result_data,
                )
                task_runs.append(task_run)

            return task_runs

    def get_workflow_durable_runs(self, workflow_run_id: UUID) -> list[UUID]:
        with self.transaction() as conn:
            task_runs = list(
                get_workflow_run_task_runs_sync(
                    conn,
                    get_workflow_run_task_runs.GetWorkflowRunTaskRunsParams(
                        workflow_run_id=workflow_run_id
                    ),
                )
            )
            # Get unique durable IDs
            durable_ids = list(set(run.durable_id for run in task_runs))
            return durable_ids

    def try_to_cancel_durable_run(self, durable_id: UUID):
        # This would need to be implemented based on your business logic
        # For now, we'll just cancel all tasks with this durable_id
        with self.transaction() as conn:
            # You might need to add a specific SQLC query for this
            # For now, using raw SQL as an example
            conn.execute(
                sqlalchemy.text(
                    "UPDATE hyrex_task_run SET status = 'CANCEL_REQUESTED'::task_run_status "
                    "WHERE durable_id = :durable_id AND status IN ('QUEUED', 'RUNNING')"
                ),
                {"durable_id": durable_id},
            )

    def update_executor_queues(self, executor_id: UUID, queues: list[str]):
        with self.transaction() as conn:
            update_queues_on_executor_sync(
                conn,
                update_queues_on_executor.UpdateQueuesOnExecutorParams(
                    id=executor_id, queues=queues
                ),
            )

    def get_result(self, task_id: UUID) -> dict:
        with self.transaction() as conn:
            result = fetch_result_sync(
                conn, fetch_result.FetchResultParams(task_id=task_id)
            )
            return result.result if result else None
