from concurrent.futures import ThreadPoolExecutor
import json
import os
import threading
import time
from datetime import datetime, timezone
from queue import Empty, Queue
from typing import Type
from uuid import UUID

import grpc
from google.protobuf.struct_pb2 import Struct
from google.protobuf import empty_pb2
from pydantic import BaseModel
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from hyrex import constants
from hyrex.configs import TaskConfig
from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.env_vars import EnvVars
from hyrex.hyrex_queue import HyrexQueue
from hyrex.proto import gateway_pb2_grpc, requests_pb2, task_pb2
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

# Define the epoch zero timestamp for comparison
EPOCH_ZERO = datetime(1970, 1, 1, tzinfo=timezone.utc)


def pydantic_aware_default(obj):
    if isinstance(obj, BaseModel):
        # If the object is a Pydantic model, call model_dump()
        # to get its dictionary representation. json.dumps can handle dicts.
        return obj.model_dump()
    # If it's not a Pydantic model and json.dumps doesn't know it,
    # let the default TypeError happen.
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


class PerformanceDispatcher(Dispatcher):
    PERFORMANCE_SERVER_HOST = os.getenv(EnvVars.PERF_SERVER_HOST, "api.hyrex.io")
    PERFORMANCE_SERVER_PORT = os.getenv(EnvVars.PERF_SERVER_PORT, "443")

    # Status mapping between Python TaskStatus enum and proto TaskStatus enum
    _PY_TO_PROTO_STATUS = {
        TaskStatus.success: task_pb2.TaskStatus.SUCCESS,
        TaskStatus.failed: task_pb2.TaskStatus.FAILED,
        TaskStatus.running: task_pb2.TaskStatus.RUNNING,
        TaskStatus.queued: task_pb2.TaskStatus.QUEUED,
        TaskStatus.up_for_cancel: task_pb2.TaskStatus.UP_FOR_CANCEL,
        TaskStatus.canceled: task_pb2.TaskStatus.CANCELED,
        TaskStatus.lost: task_pb2.TaskStatus.LOST,
        TaskStatus.stopped: task_pb2.TaskStatus.STOPPED,
        TaskStatus.skipped: task_pb2.TaskStatus.SKIPPED,
        TaskStatus.await_deps: task_pb2.TaskStatus.AWAIT_DEPS,
        TaskStatus.await_start_time: task_pb2.TaskStatus.AWAIT_START_TIME,
    }

    # Reverse mapping for proto to Python conversion
    _PROTO_TO_PY_STATUS = {v: k for k, v in _PY_TO_PROTO_STATUS.items()}

    # Priority mapping between Python int and proto Priority enum
    _PY_TO_PROTO_PRIORITY = {
        0: task_pb2.Priority.P_UNSPECIFIED,
        1: task_pb2.Priority.P1,
        2: task_pb2.Priority.P2,
        3: task_pb2.Priority.P3,
        4: task_pb2.Priority.P4,
        5: task_pb2.Priority.P5,
        6: task_pb2.Priority.P6,
        7: task_pb2.Priority.P7,
        8: task_pb2.Priority.P8,
        9: task_pb2.Priority.P9,
        10: task_pb2.Priority.P10,
    }

    def __init__(self, api_key: str, conn_string: str):
        # def __init__(self, api_key: str, batch_size=100, flush_interval=0.1):
        super().__init__()

        self.api_key = api_key
        self.api_key_metadata = (("x-api-key", self.api_key),)

        server_address = (
            f"{self.PERFORMANCE_SERVER_HOST}:{self.PERFORMANCE_SERVER_PORT}"
        )

        if os.getenv(EnvVars.LOCAL_TESTING):
            self.logger.info("Testing locally.")
            self.channel = grpc.insecure_channel(server_address)
        else:
            channel_credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(server_address, channel_credentials)
        self.gateway_stub = gateway_pb2_grpc.GatewayServiceStub(self.channel)

        # TODO: Consider setting max workers specifically here.
        self.enqueue_executor = ThreadPoolExecutor()
        self.running = True

        self.register_shutdown_handlers()

    def register_app(self, app_info: dict):
        app_info_struct = Struct()
        app_info_struct.update(app_info)

        request_proto = requests_pb2.RegisterAppRequest()
        request_proto.app_info = app_info_struct

        try:
            response = self.gateway_stub.RegisterApp(
                request_proto, metadata=self.api_key_metadata
            )
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def _convert_enqueue_request_to_proto(
        self, task: EnqueueTaskRequest
    ) -> requests_pb2.EnqueueRequest:
        """
        Convert an EnqueueTaskRequest to a proto EnqueueRequest message.
        """
        proto_task = requests_pb2.EnqueueRequest()
        proto_task.id = str(task.id)
        proto_task.durable_id = str(task.id)
        proto_task.root_id = str(task.root_id)
        if task.parent_id:
            proto_task.parent_id = str(task.parent_id)
        if task.workflow_run_id:
            proto_task.workflow_run_id = str(task.workflow_run_id)
        if task.workflow_dependencies:
            proto_task.workflow_dependencies.extend(
                [str(dep) for dep in task.workflow_dependencies]
            )
        proto_task.task_name = task.task_name
        proto_task.queue = task.queue
        proto_task.max_retries = task.max_retries
        proto_task.priority = task.priority
        if task.timeout_seconds is not None:
            proto_task.timeout_seconds = task.timeout_seconds
        if task.idempotency_key:
            proto_task.idempotency_key = task.idempotency_key

        try:
            # json.dumps will handle dicts, lists, strings, numbers etc. directly.
            # If it encounters a Pydantic model (either as task.args itself or nested),
            # it will call our pydantic_aware_default function.
            json_string = json.dumps(task.args, default=pydantic_aware_default)
            proto_task.args = json_string.encode("utf-8")
        except TypeError as e:
            self.logger.error(f"Task {task.id}: Failed to serialize args to JSON: {e}")
            raise

        return proto_task

    @retry(
        stop=stop_after_attempt(4),  # Try up to 4 times (initial + 3 retries)
        wait=wait_exponential(
            multiplier=1, min=1, max=30
        ),  # Exponential backoff: 1s, 2s, 4s, 8s... capped at 30s
        retry=retry_if_exception_type(grpc.RpcError),
    )
    def _send_grpc_enqueue_request(self, proto_task: requests_pb2.EnqueueRequest):
        """
        Send a single gRPC enqueue request synchronously with automatic retry.
        This method is designed to be called from within the ThreadPoolExecutor.
        Tenacity handles the retry logic with exponential backoff.
        """
        try:
            response = self.gateway_stub.Enqueue(
                proto_task, metadata=self.api_key_metadata
            )
            return response
        except grpc.RpcError as e:
            # Only retry on certain error codes
            retryable_codes = [
                grpc.StatusCode.UNAVAILABLE,
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.RESOURCE_EXHAUSTED,
                grpc.StatusCode.ABORTED,
                grpc.StatusCode.INTERNAL,
            ]

            if e.code() not in retryable_codes:
                self.logger.error(
                    f"Non-retryable gRPC error: {e.code()} - {e.details()}"
                )
                raise  # This won't be retried by tenacity

            self.logger.warning(
                f"Retryable gRPC error: {e.code()} - {e.details()}. Retrying..."
            )
            raise  # This will be retried by tenacity

    def _send_grpc_enqueue_callback(self, future):
        """
        Callback function to handle the result of an async gRPC request.
        """
        try:
            result = future.result()
            # Log success or process result as needed
            self.logger.debug(f"Enqueue request completed successfully")
        except Exception as e:
            # Log the error but don't re-raise to avoid crashing the executor
            self.logger.error(f"Enqueue request failed: {e}")

    def enqueue(self, tasks: list[EnqueueTaskRequest]):
        for task in tasks:
            proto_task = self._convert_enqueue_request_to_proto(task)
            # Submit the gRPC request to the thread pool for async execution
            future = self.enqueue_executor.submit(
                self._send_grpc_enqueue_request, proto_task
            )
            # Add callback to handle the result
            future.add_done_callback(self._send_grpc_enqueue_callback)

    def dequeue(
        self,
        executor_id: UUID,
        task_names: list[str],
        queue: str = constants.ANY_QUEUE,
        concurrency_limit: int = 0,
    ) -> DequeuedTask:
        request_proto = requests_pb2.DequeueRequest()
        request_proto.queue = queue
        request_proto.executor_id = str(executor_id)

        try:
            response = self.gateway_stub.Dequeue(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug(f"gRPC call successful, response {response}")
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

        # No task found
        # Check for task.id instead of just task because sometimes Python parses this as an instantiated empty task
        if not response.task_run.id:
            return None

        task_run = response.task_run

        # Parse args from bytes to dict
        args_dict = {}
        if task_run.args:
            try:
                args_dict = json.loads(task_run.args.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                self.logger.error(f"Failed to decode task args: {e}")

        # Convert protobuf timestamp to datetime
        queued_time = datetime.fromtimestamp(task_run.queued.seconds)
        started_time = datetime.fromtimestamp(task_run.started.seconds)

        scheduled_start = None
        if task_run.HasField("scheduled_start"):
            scheduled_start = datetime.fromtimestamp(task_run.scheduled_start.seconds)

        timeout_seconds = None
        if task_run.HasField("timeout_seconds"):
            timeout_seconds = task_run.timeout_seconds

        workflow_run_id = None
        if task_run.HasField("workflow_run_id"):
            workflow_run_id = UUID(task_run.workflow_run_id)

        parent_id = None
        if task_run.parent_id:
            parent_id = UUID(task_run.parent_id)

        return DequeuedTask(
            id=UUID(task_run.id),
            durable_id=UUID(task_run.durable_id),
            root_id=UUID(task_run.root_id),
            parent_id=parent_id,
            task_name=task_run.task_name,
            args=args_dict,
            queue=task_run.queue,
            priority=task_run.priority,
            timeout_seconds=timeout_seconds,
            scheduled_start=scheduled_start,
            queued=queued_time,
            started=started_time,
            workflow_run_id=workflow_run_id,
            attempt_number=task_run.attempt_number,
            max_retries=task_run.max_retries,
        )

    # def enqueue(
    #     self,
    #     tasks: list[EnqueueTaskRequest],
    # ):
    #     for task in tasks:
    #         self.local_queue.put(task)

    # def _batch_enqueue(self):
    #     tasks = []
    #     last_flush_time = time.monotonic()
    #     while self.running or not self.local_queue.empty():
    #         timeout = self.flush_interval - (time.monotonic() - last_flush_time)
    #         if timeout <= 0:
    #             # Flush if the flush interval has passed
    #             if tasks:
    #                 self._enqueue_tasks(tasks)
    #                 tasks = []
    #             last_flush_time = time.monotonic()
    #             continue

    #         try:
    #             # Wait for a task or until the timeout expires
    #             task = self.local_queue.get(timeout=timeout)
    #             tasks.append(task)
    #             if len(tasks) >= self.batch_size:
    #                 # Flush if batch size is reached
    #                 self._enqueue_tasks(tasks)
    #                 tasks = []
    #                 last_flush_time = time.monotonic()
    #         except Empty:
    #             # No task received within the timeout
    #             if tasks:
    #                 self._enqueue_tasks(tasks)
    #                 tasks = []
    #             last_flush_time = time.monotonic()

    #     # Flush any remaining tasks when stopping
    #     if tasks:
    #         self._enqueue_tasks(tasks)

    # def _enqueue_tasks(self, tasks: list[EnqueueTaskRequest]):
    #     enqueue_url = f"{self.HYREX_PLATFORM_URL}{self.ENQUEUE_TASK_PATH}"
    #     headers = {
    #         "X-API-Key": self.api_key,
    #     }

    #     task_list_json = [task.model_dump(mode="json") for task in tasks]

    #     try:
    #         response = requests.post(enqueue_url, headers=headers, json=task_list_json)
    #         if response.status_code != 200:
    #             self.logger.error(f"Error enqueuing task: {response.status_code}")
    #             self.logger.error(f"Response body: {response.text}")
    #     except requests.exceptions.RequestException as e:
    #         self.logger.error(f"Error enqueuing task via API: {str(e)}")
    #         raise RuntimeError(f"Failed to enqueue task via API: {e}")

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        # Check if already stopping/stopped
        if not self.running:
            return

        self.logger.debug("Stopping dispatcher...")
        self.running = False
        self.enqueue_executor.shutdown(wait=True)
        if self.channel:
            self.channel.close()
        self.logger.debug("Dispatcher stopped successfully!")

    def mark_success(self, task_id: UUID, result: str):
        request_proto = requests_pb2.MarkSuccessRequest()
        request_proto.task_run_id = str(task_id)
        if result:
            request_proto.result = result

        try:
            self.gateway_stub.MarkSuccess(request_proto, metadata=self.api_key_metadata)
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC MarkSuccess call failed: {e.code()} - {e.details()}"
            )
            raise

    def mark_failed(self, task_id: UUID):
        request_proto = requests_pb2.MarkFailedRequest()
        request_proto.task_run_id = str(task_id)

        try:
            self.gateway_stub.MarkFailed(request_proto, metadata=self.api_key_metadata)
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC MarkFailed call failed: {e.code()} - {e.details()}"
            )
            raise

    def retry_task(self, task_id: UUID, backoff_seconds: int):
        request_proto = requests_pb2.RetryTaskRunRequest()
        request_proto.task_run_id = str(task_id)
        request_proto.backoff_seconds = backoff_seconds

        try:
            self.gateway_stub.RetryTaskRun(
                request_proto, metadata=self.api_key_metadata
            )
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC RetryTaskRun call failed: {e.code()} - {e.details()}"
            )
            raise

    # TODO: Implement
    def try_to_cancel_task(self, task_id: UUID):
        raise NotImplementedError("Cancellation not yet implemented on Hyrex platform")

    def task_canceled(self, task_id: UUID):
        request_proto = requests_pb2.MarkCanceledRequest()
        request_proto.task_run_id = str(task_id)

        try:
            self.gateway_stub.MarkCanceled(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("MarkCanceled gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC MarkCanceled call failed: {e.code()} - {e.details()}"
            )
            raise

    def get_task_status(self, task_id: UUID) -> TaskStatus:
        request_proto = requests_pb2.GetTaskRunStatusRequest()
        request_proto.task_run_id = str(task_id)

        try:
            response = self.gateway_stub.GetTaskStatus(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug(
                f"gRPC GetTaskStatus call successful, response: {response}"
            )
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC GetTaskStatus call failed: {e.code()} - {e.details()}"
            )
            raise

        return self._PROTO_TO_PY_STATUS[response.status]

    def register_executor(
        self,
        executor_id: UUID,
        executor_name: str,
        queue_pattern: str,
        queues: list[HyrexQueue],
        worker_name: str,
    ):
        request_proto = requests_pb2.RegisterExecutorRequest()
        request_proto.executor_id = str(executor_id)
        request_proto.executor_name = executor_name
        request_proto.queue_pattern = queue_pattern
        request_proto.queues.extend([queue.name for queue in queues])
        request_proto.worker_name = worker_name

        try:
            self.gateway_stub.RegisterExecutor(
                request_proto, metadata=self.api_key_metadata
            )
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC RegisterExecutor call failed: {e.code()} - {e.details()}"
            )
            raise

    def disconnect_executor(self, executor_id: UUID):
        request_proto = requests_pb2.DisconnectExecutorRequest()
        request_proto.executor_id = str(executor_id)

        try:
            self.gateway_stub.DisconnectExecutor(
                request_proto, metadata=self.api_key_metadata
            )
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC DisconnectExecutor call failed: {e.code()} - {e.details()}"
            )
            raise

    def mark_running_tasks_lost(self, executor_id: UUID):
        request_proto = requests_pb2.MarkRunningTasksLostRequest()
        request_proto.executor_id = str(executor_id)

        try:
            self.gateway_stub.MarkRunningTasksLost(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("MarkRunningTasksLost gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC MarkRunningTasksLost call failed: {e.code()} - {e.details()}"
            )
            raise

    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        request_proto = requests_pb2.ExecutorHeartbeatRequest()
        request_proto.executor_ids.extend(
            [str(executor_id) for executor_id in executor_ids]
        )

        # Convert datetime to protobuf timestamp
        request_proto.timestamp.FromDatetime(timestamp)

        try:
            self.gateway_stub.ExecutorHeartbeat(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("ExecutorHeartbeat gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC ExecutorHeartbeat call failed: {e.code()} - {e.details()}"
            )
            raise

    def update_executor_stats(self, executor_id: UUID, stats: dict):
        request_proto = requests_pb2.UpdateExecutorStatsRequest()
        request_proto.executor_id = str(executor_id)

        # Convert dict to protobuf Struct
        stats_struct = Struct()
        stats_struct.update(stats)
        request_proto.executor_stats.CopyFrom(stats_struct)

        try:
            self.gateway_stub.UpdateExecutorStats(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("UpdateExecutorStats gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC UpdateExecutorStats call failed: {e.code()} - {e.details()}"
            )
            raise

    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        request_proto = requests_pb2.TaskRunHeartbeatRequest()
        request_proto.task_run_ids.extend([str(task_id) for task_id in task_ids])

        # Convert datetime to protobuf timestamp
        request_proto.timestamp.FromDatetime(timestamp)

        try:
            self.gateway_stub.TaskRunHeartbeat(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("TaskRunHeartbeat gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC TaskRunHeartbeat call failed: {e.code()} - {e.details()}"
            )
            raise

    def get_tasks_up_for_cancel(self) -> list[UUID]:
        try:
            response = self.gateway_stub.GetTaskRunsUpForCancel(
                empty_pb2.Empty(), metadata=self.api_key_metadata
            )
            self.logger.debug(
                f"GetTaskRunsUpForCancel gRPC call successful, response: {response}"
            )

            # Convert string UUIDs to UUID objects
            return [UUID(task_id) for task_id in response.task_run_ids]
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC GetTaskRunsUpForCancel call failed: {e.code()} - {e.details()}"
            )
            raise

    def get_queues_for_pattern(self, pattern: QueuePattern) -> list[str]:
        request_proto = requests_pb2.GetQueuesRequest()
        request_proto.max_num_queues = 10000
        request_proto.pattern = pattern.glob_pattern

        try:
            response = self.gateway_stub.GetQueues(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug(f"GetQueues gRPC call successful. response: {response}")
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

        return response.queues

    def register_task_def(
        self,
        task_name: str,
        arg_schema: Type[BaseModel] | None,
        task_config: TaskConfig,
        cron: str = None,
        source_code: str = None,
    ):
        # The proto structure has changed to use a Task message
        request_proto = requests_pb2.RegisterTaskDefRequest()

        # Create the TaskDef message
        task_def = task_pb2.TaskDef()
        task_def.task_name = task_name

        # Handle arg_schema
        if arg_schema and hasattr(arg_schema, "model_json_schema"):
            arg_schema_struct = Struct()
            arg_schema_struct.update(arg_schema.model_json_schema())
            task_def.arg_schema.CopyFrom(arg_schema_struct)

        # Set required fields from task_config
        task_def.queue = (
            task_config.get_queue_name() if task_config.queue else "default"
        )
        task_def.max_retries = (
            task_config.max_retries if task_config.max_retries is not None else 0
        )

        # Map Python priority to protobuf Priority enum
        priority_value = task_config.priority if task_config.priority is not None else 5
        task_def.priority = self._PY_TO_PROTO_PRIORITY.get(
            priority_value, task_pb2.Priority.P5
        )

        # Set optional fields
        if task_config.timeout_seconds is not None:
            task_def.timeout_seconds = task_config.timeout_seconds

        if cron:
            task_def.cron = cron

        if source_code:
            task_def.source_code = source_code

        # Set the task in the request proto
        request_proto.task_def.CopyFrom(task_def)

        try:
            self.gateway_stub.RegisterTaskDef(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC RegisterTaskDef call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC RegisterTaskDef call failed: {e.code()} - {e.details()}"
            )
            raise

    def acquire_scheduler_lock(self, worker_name: str) -> int | None:
        pass

    def pull_cron_job_expressions(self) -> list[CronJob]:
        return []

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

    def execute_queued_cron_job_run(self) -> str | None:
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
        cron: str | None,
    ):
        request_proto = requests_pb2.RegisterWorkflowRequest()
        request_proto.workflow_name = name
        request_proto.source_code = source_code

        # Handle workflow_dag_json - convert dict to JSON string if needed
        if isinstance(workflow_dag_json, dict):
            request_proto.workflow_dag_json = json.dumps(workflow_dag_json)
        else:
            request_proto.workflow_dag_json = workflow_dag_json

        # Handle arg_schema
        if workflow_arg_schema:
            arg_schema_struct = Struct()
            # Convert Pydantic BaseModel class to JSON schema dict
            if hasattr(workflow_arg_schema, "model_json_schema"):
                schema_dict = workflow_arg_schema.model_json_schema()
                arg_schema_struct.update(schema_dict)
            else:
                # If it's already a dict or other type, use it directly
                arg_schema_struct.update(workflow_arg_schema)
            request_proto.workflow_arg_schema.CopyFrom(arg_schema_struct)

        # Handle default_config
        if default_config:
            default_config_struct = Struct()
            default_config_struct.update(default_config)
            request_proto.default_config.CopyFrom(default_config_struct)

        # Handle cron
        if cron:
            request_proto.cron = cron

        try:
            self.gateway_stub.RegisterWorkflow(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC RegisterWorkflow call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC RegisterWorkflow call failed: {e.code()} - {e.details()}"
            )
            raise

    def send_workflow_run(self, workflow_run_request: WorkflowRunRequest) -> UUID:
        request_proto = requests_pb2.SendWorkflowRunRequest()
        request_proto.workflow_run_id = str(workflow_run_request.id)
        request_proto.workflow_name = workflow_run_request.workflow_name
        request_proto.queue = workflow_run_request.queue

        # Convert args to protobuf Struct
        args_struct = Struct()
        args_struct.update(workflow_run_request.args)
        request_proto.args.CopyFrom(args_struct)

        # Handle optional fields
        if workflow_run_request.timeout_seconds is not None:
            request_proto.timeout_seconds = workflow_run_request.timeout_seconds

        if workflow_run_request.idempotency_key:
            request_proto.idempotency_key = workflow_run_request.idempotency_key

        try:
            self.gateway_stub.SendWorkflowRun(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC SendWorkflowRun call successful")
            return workflow_run_request.id
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC SendWorkflowRun call failed: {e.code()} - {e.details()}"
            )
            raise

    def advance_workflow_run(self, workflow_run_id: UUID):
        request_proto = requests_pb2.AdvanceWorkflowRunRequest()
        request_proto.workflow_run_id = str(workflow_run_id)

        try:
            self.gateway_stub.AdvanceWorkflowRun(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC AdvanceWorkflowRun call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC AdvanceWorkflowRun call failed: {e.code()} - {e.details()}"
            )
            raise

    def get_workflow_run_args(self, workflow_run_id: UUID) -> dict:
        request_proto = requests_pb2.GetWorkflowRunArgsRequest()
        request_proto.workflow_run_id = str(workflow_run_id)

        try:
            response = self.gateway_stub.GetWorkflowRunArgs(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC GetWorkflowRunArgs call successful")

            # Convert protobuf Struct to dict
            return dict(response.args)
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC GetWorkflowRunArgs call failed: {e.code()} - {e.details()}"
            )
            raise

    def get_durable_run_tasks(self, durable_id: UUID) -> list[TaskRun]:
        request_proto = requests_pb2.GetDurableTaskRunsRequest()
        request_proto.durable_id = str(durable_id)

        try:
            response = self.gateway_stub.GetDurableTaskRuns(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug(f"gRPC call successful, response: {response}")
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

        # This helps handle immediate `wait()` calls when task is sent, but not yet processed by performance server.
        # TODO: Handle this better!
        if not response.task_runs:
            time.sleep(0.5)
            return self.get_durable_run_tasks(durable_id=durable_id)

        python_task_runs = []
        for proto_task in response.task_runs:
            try:
                # Map protobuf enum to Python StrEnum using the mapping dict
                current_status = self._PROTO_TO_PY_STATUS.get(proto_task.status)
                if current_status is None:
                    # Fallback: try to convert protobuf enum name to Python enum
                    status_name = task_pb2.TaskStatus.Name(proto_task.status)
                    self.logger.warning(
                        f"Unknown proto status {proto_task.status} ({status_name})"
                    )
                    current_status = TaskStatus(
                        status_name
                    )  # This will raise if not found

                # Convert timestamps, checking for epoch zero (default/unset) for optional Python fields
                queued_dt = proto_task.queued.ToDatetime(
                    tzinfo=timezone.utc
                )  # Store with UTC timezone
                started_dt = proto_task.started.ToDatetime(tzinfo=timezone.utc)
                finished_dt = proto_task.finished.ToDatetime(tzinfo=timezone.utc)

                queued_val = queued_dt if queued_dt > EPOCH_ZERO else None
                started_val = started_dt if started_dt > EPOCH_ZERO else None
                finished_val = finished_dt if finished_dt > EPOCH_ZERO else None

                # Parse JSON result string
                task_result = (
                    json.loads(proto_task.result) if proto_task.result else None
                )

                python_task = TaskRun(
                    id=UUID(proto_task.id),
                    task_name=proto_task.task_name,
                    max_retries=proto_task.max_retries,
                    attempt_number=proto_task.attempt_number,
                    status=current_status,
                    queued=queued_val,
                    started=started_val,
                    finished=finished_val,
                    result=task_result,
                )
                python_task_runs.append(python_task)

            except (ValueError, json.JSONDecodeError, KeyError) as e:
                # Handle potential errors during conversion (e.g., invalid UUID, bad JSON, invalid status enum)
                self.logger.error(f"Error converting TaskRun {proto_task.id}: {e}")
                # TODO: Decide how to proceed: skip this task, raise an error, etc.
                raise

        return python_task_runs

    def get_workflow_durable_runs(self, workflow_run_id: UUID) -> list[UUID]:
        request_proto = requests_pb2.GetWorkflowDurableRunsRequest()
        request_proto.workflow_run_id = str(workflow_run_id)

        try:
            response = self.gateway_stub.GetWorkflowDurableRuns(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC GetWorkflowDurableRuns call successful")

            # Convert string UUIDs to UUID objects
            return [UUID(durable_id) for durable_id in response.durable_ids]
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC GetWorkflowDurableRuns call failed: {e.code()} - {e.details()}"
            )
            raise

    def try_to_cancel_durable_run(self, durable_id: UUID):
        request_proto = requests_pb2.TryToCancelDurableRunRequest()
        request_proto.durable_id = str(durable_id)

        try:
            self.gateway_stub.TryToCancelDurableRun(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("TryToCancelDurableRun gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC TryToCancelDurableRun call failed: {e.code()} - {e.details()}"
            )
            raise

    def update_executor_queues(self, executor_id: UUID, queues: list[str]):
        request_proto = requests_pb2.UpdateExecutorQueuesRequest()
        request_proto.executor_id = str(executor_id)
        request_proto.queues.extend(
            queues
        )  # Use extend for repeated field instead of direct assignment

        try:
            self.gateway_stub.UpdateExecutorQueues(
                request_proto, metadata=self.api_key_metadata
            )
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def save_result(self, task_id: UUID, result: str):
        request_proto = requests_pb2.SaveTaskRunResultRequest()
        request_proto.task_run_id = str(task_id)
        request_proto.result = result

        try:
            self.gateway_stub.SaveTaskResult(
                request_proto, metadata=self.api_key_metadata
            )
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def get_result(self, task_id: UUID) -> dict:
        request_proto = requests_pb2.GetTaskRunResultRequest()
        request_proto.task_run_id = str(task_id)

        try:
            response = self.gateway_stub.GetTaskResult(
                request_proto, metadata=self.api_key_metadata
            )
            return json.loads(response.result)
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def set_log_link(self, task_id: UUID, log_link: str):
        request_proto = requests_pb2.SetLogLinkRequest()
        request_proto.task_run_id = str(task_id)
        request_proto.log_link = log_link

        try:
            self.gateway_stub.SetLogLink(request_proto, metadata=self.api_key_metadata)
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def write_s3_logs(self, task_id: UUID, logs: str):
        request_proto = requests_pb2.WriteLogsRequest()
        request_proto.task_run_id = str(task_id)
        request_proto.logs = logs

        try:
            self.gateway_stub.WriteLogs(request_proto, metadata=self.api_key_metadata)
            self.logger.debug("WriteLogs gRPC call successful")
        except grpc.RpcError as e:
            self.logger.error(f"gRPC WriteLogs call failed: {e.code()} - {e.details()}")
            raise
