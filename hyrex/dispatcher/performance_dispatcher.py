import json
import os
import threading
import time
from datetime import datetime, timezone
from queue import Empty, Queue
from typing import Type
from uuid import UUID

import grpc
import requests
from google.protobuf.struct_pb2 import Struct
from pydantic import BaseModel

from hyrex import constants
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
        TaskStatus.queued: task_pb2.TaskStatus.QUEUED,
        TaskStatus.waiting: task_pb2.TaskStatus.WAITING,
        TaskStatus.running: task_pb2.TaskStatus.RUNNING,
        TaskStatus.success: task_pb2.TaskStatus.SUCCESS,
        TaskStatus.failed: task_pb2.TaskStatus.FAILED,
        TaskStatus.up_for_cancel: task_pb2.TaskStatus.UP_FOR_CANCEL,
        TaskStatus.canceled: task_pb2.TaskStatus.CANCELED,
        TaskStatus.lost: task_pb2.TaskStatus.LOST,
        TaskStatus.skipped: task_pb2.TaskStatus.SKIPPED,
    }

    # Reverse mapping for proto to Python conversion
    _PROTO_TO_PY_STATUS = {v: k for k, v in _PY_TO_PROTO_STATUS.items()}

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

        # TODO: Bring these back if we switch to batching of enqueues
        # self.local_queue = Queue()
        self.running = True
        # self.batch_size = batch_size
        # self.flush_interval = flush_interval

        # self.thread = threading.Thread(target=self._batch_enqueue, daemon=True)
        # self.thread.start()

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
            print("gRPC call successful, response:", response)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def enqueue(self, tasks: list[EnqueueTaskRequest]):
        for task in tasks:
            proto_task = requests_pb2.EnqueueTaskRequest()
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
                self.logger.error(
                    f"Task {task.id}: Failed to serialize args to JSON: {e}"
                )
                raise

            proto_task.status = task_pb2.TaskStatus.QUEUED

            try:
                start_time = time.perf_counter()
                try:
                    response = self.gateway_stub.Enqueue(
                        proto_task, metadata=self.api_key_metadata
                    )
                    print("gRPC call successful, response:", response)
                except grpc.RpcError as e:
                    print(f"gRPC call failed: {e.code()} - {e.details()}")
                    raise
            finally:
                print(
                    f"Enqueue request round-trip duration: {time.perf_counter() - start_time} seconds"
                )

    def dequeue(
        self,
        executor_id: UUID,
        task_names: list[str],
        queue: str = constants.ANY_QUEUE,
        concurrency_limit: int = 0,
    ) -> DequeuedTask:
        request_proto = requests_pb2.DequeueTaskRequest()
        request_proto.queue = queue

        try:
            start_time = time.perf_counter()
            try:
                response = self.gateway_stub.Dequeue(
                    request_proto, metadata=self.api_key_metadata
                )
                print("gRPC call successful, response:", response)
            except grpc.RpcError as e:
                print(f"gRPC call failed: {e.code()} - {e.details()}")
                raise
        finally:
            print(
                f"Dequeue request round-trip duration: {time.perf_counter() - start_time} seconds"
            )

        # No task found
        # Check for task.id instead of just task because sometimes Python parses this as an instantiated empty task
        if not response.task.id:
            return None

        task_run = response.task
        print(f"{task_run}")

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
        self.logger.info("Stopping dispatcher...")
        self.running = False
        self.channel.close()
        self.logger.info("Dispatcher stopped successfully!")

    def mark_success(self, task_id: UUID, result: str):
        request_proto = requests_pb2.MarkSuccessRequest()
        request_proto.task_id = str(task_id)
        if result:
            request_proto.result = result

        try:
            response = self.gateway_stub.MarkSuccess(
                request_proto, metadata=self.api_key_metadata
            )
            print(f"gRPC MarkSuccess call successful, response: {response}")
        except grpc.RpcError as e:
            print(f"gRPC MarkSuccess call failed: {e.code()} - {e.details()}")
            raise

    def mark_failed(self, task_id: UUID):
        request_proto = requests_pb2.MarkFailedRequest()
        request_proto.task_id = str(task_id)

        try:
            response = self.gateway_stub.MarkFailed(
                request_proto, metadata=self.api_key_metadata
            )
            print(f"gRPC MarkFailed call successful, response: {response}")
        except grpc.RpcError as e:
            print(f"gRPC MarkFailed call failed: {e.code()} - {e.details()}")
            raise

    def retry_task(self, task_id: UUID, backoff_seconds: int):
        # TODO: Implement
        self.logger.info("WOULD HAVE RETRIED!")
        pass

    # TODO: Implement
    def try_to_cancel_task(self, task_id: UUID):
        raise NotImplementedError("Cancellation not yet implemented on Hyrex platform")

    def task_canceled(self, task_id: UUID):
        raise NotImplementedError("Cancellation not yet implemented on Hyrex platform")

    def get_task_status(self, task_id: UUID) -> TaskStatus:
        request_proto = requests_pb2.GetTaskStatusRequest()
        request_proto.task_id = str(task_id)

        try:
            start_time = time.perf_counter()
            try:
                response = self.gateway_stub.GetTaskStatus(
                    request_proto, metadata=self.api_key_metadata
                )
                print(f"gRPC GetTaskStatus call successful, response: {response}")
            except grpc.RpcError as e:
                print(f"gRPC GetTaskStatus call failed: {e.code()} - {e.details()}")
                raise
        finally:
            print(
                f"GetTaskStatus request round-trip duration: {time.perf_counter() - start_time} seconds"
            )

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
            start_time = time.perf_counter()
            try:
                response = self.gateway_stub.RegisterExecutor(
                    request_proto, metadata=self.api_key_metadata
                )
                print(
                    f"gRPC RegisterExecutor call successful, response: {response.message}"
                )
            except grpc.RpcError as e:
                print(f"gRPC RegisterExecutor call failed: {e.code()} - {e.details()}")
                raise
        finally:
            print(
                f"RegisterExecutor request round-trip duration: {time.perf_counter() - start_time} seconds"
            )

    def disconnect_executor(self, executor_id: UUID):
        pass

    def mark_running_tasks_lost(self, executor_id: UUID):
        # TODO: Implement
        pass

    def executor_heartbeat(self, executor_ids: list[UUID], timestamp: datetime):
        pass

    def update_executor_stats(self, executor_id: UUID, stats: dict):
        pass

    def task_heartbeat(self, task_ids: list[UUID], timestamp: datetime):
        # TODO: Implement
        pass

    def get_tasks_up_for_cancel(self) -> list[UUID]:
        # TODO: Implement
        return []

    def get_queues_for_pattern(self, pattern: QueuePattern) -> list[str]:
        request_proto = requests_pb2.GetQueuesRequest()
        request_proto.max_num_queues = 10000
        request_proto.pattern = pattern.glob_pattern

        try:
            response = self.gateway_stub.GetQueues(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug(response)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

        return response.queues

    def register_task(
        self,
        task_name: str,
        arg_schema: Type[BaseModel] | None,
        default_config: dict,
        cron: str = None,
        source_code: str = None,
    ):
        # The proto structure has changed to use a Task message
        request_proto = requests_pb2.RegisterTaskDefRequest()

        # Create the Task message
        task = task_pb2.Task()
        task.task_name = task_name

        # Handle arg_schema
        if arg_schema:
            arg_schema_struct = Struct()
            arg_schema_struct.update(arg_schema)
            task.arg_schema.CopyFrom(arg_schema_struct)

        # Handle default_config
        if default_config:
            default_config_struct = Struct()
            default_config_struct.update(default_config)
            task.default_config.CopyFrom(default_config_struct)

        # Set optional fields
        if cron:
            task.cron = cron

        if source_code:
            task.source_code = source_code

        # Set the task in the request proto
        request_proto.task.CopyFrom(task)

        try:
            response = self.gateway_stub.RegisterTaskDef(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug("gRPC RegisterTaskDef call successful")
        except grpc.RpcError as e:
            self.logger.error(
                f"gRPC RegisterTaskDef call failed: {e.code()} - {e.details()}"
            )
            raise

    def acquire_scheduler_lock(self, worker_name: str) -> int | None:
        request_proto = requests_pb2.AcquireSchedulerLockRequest()
        request_proto.worker_name = worker_name

        try:
            response = self.gateway_stub.AcquireSchedulerLock(
                request_proto, metadata=self.api_key_metadata
            )
            self.logger.debug(response)
            return response.lock_id
        except grpc.RpcError as e:
            self.logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

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
    ):
        pass

    def send_workflow_run(self, workflow_run_request: WorkflowRunRequest) -> UUID:
        pass

    def advance_workflow_run(self, workflow_run_id: UUID):
        pass

    def get_workflow_run_args(self, workflow_run_id: UUID) -> dict:
        pass

    def get_durable_run_tasks(self, durable_id: UUID) -> list[TaskRun]:
        request_proto = requests_pb2.GetDurableRunTasksRequest()
        request_proto.durable_id = str(durable_id)

        try:
            response = self.gateway_stub.GetDurableTasks(
                request_proto, metadata=self.api_key_metadata
            )
            print("gRPC call successful, response:", response.message)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

        python_task_runs = []
        for proto_task in response.tasks:
            try:
                # Map protobuf enum name to Python StrEnum.
                # Assumes the enum names in proto match the values in Python's TaskStatus.
                # Example: If proto has TASK_STATUS_SUCCESS = 0, .Name() might return "TASK_STATUS_SUCCESS".
                # Adjust the mapping logic if needed based on your actual enum definitions.
                # If the proto enum names are exactly the Python values (e.g., "success"), this is simpler.
                # Let's assume direct mapping for now:
                status_str = task_pb2.TaskStatus.Name(
                    proto_task.status
                ).lower()  # Or adjust based on actual names
                # Handle potential prefix if needed, e.g., status_str = status_str.replace('task_status_', '')
                current_status = TaskStatus(
                    status_str
                )  # Convert string name to Python Enum

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
                print(f"Error converting TaskRun {proto_task.id}: {e}")
                # TODO: Decide how to proceed: skip this task, raise an error, etc.
                raise

        print(python_task_runs)
        return python_task_runs

    def get_workflow_durable_runs(self, workflow_run_id: UUID) -> list[UUID]:
        pass

    def try_to_cancel_durable_run(self, durable_id: UUID):
        pass

    def update_executor_queues(self, executor_id: UUID, queues: list[str]):
        request_proto = requests_pb2.UpdateExecutorQueuesRequest()
        request_proto.executor_id = str(executor_id)
        request_proto.queues.extend(
            queues
        )  # Use extend for repeated field instead of direct assignment

        try:
            response = self.gateway_stub.UpdateExecutorQueues(
                request_proto, metadata=self.api_key_metadata
            )
            print("gRPC call successful, response:", response.message)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def save_result(self, task_id: UUID, result: str):
        request_proto = requests_pb2.SaveTaskResultRequest()
        request_proto.task_id = str(task_id)
        request_proto.result = result

        try:
            response = self.gateway_stub.SaveTaskResult(
                request_proto, metadata=self.api_key_metadata
            )
            print("gRPC call successful, response:", response.message)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def get_result(self, task_id: UUID) -> dict:
        request_proto = requests_pb2.GetTaskResultRequest()
        request_proto.task_id = str(task_id)

        try:
            response = self.gateway_stub.GetTaskResult(
                request_proto, metadata=self.api_key_metadata
            )
            print("gRPC call successful, response:", response.message)
            return json.loads(response.result)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise

    def set_log_link(self, task_id: UUID, log_link: str):
        request_proto = requests_pb2.SetLogLinkRequest()
        request_proto.task_id = str(task_id)
        request_proto.log_link = log_link

        try:
            response = self.gateway_stub.SetLogLink(
                request_proto, metadata=self.api_key_metadata
            )
            print("gRPC call successful, response:", response.message)
        except grpc.RpcError as e:
            print(f"gRPC call failed: {e.code()} - {e.details()}")
            raise
