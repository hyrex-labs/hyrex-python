from typing import Type
from uuid import UUID

from pydantic import BaseModel
from uuid6 import uuid7

from hyrex.configs import ConfigPhase, TaskConfig, WorkflowConfig
from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.hyrex_queue import HyrexQueue
from hyrex.schemas import EnqueueTaskRequest, TaskStatus, WorkflowRunRequest
from hyrex.workflow.workflow_builder import DagNode, WorkflowBuilder


class HyrexWorkflow:
    def __init__(
        self,
        name: str,
        workflow_config: WorkflowConfig,
        workflow_arg_schema: Type[BaseModel] | None,
        workflow_builder: WorkflowBuilder,
        dispatcher: Dispatcher,
        source_code: str,
        cron: str | None,
    ):
        self.name = name
        self.workflow_config = workflow_config
        self.workflow_arg_schema = workflow_arg_schema
        self.workflow_builder = workflow_builder
        self.dispatcher = dispatcher
        self.source_code = source_code
        self.cron = cron

    def get_queue(self) -> HyrexQueue | str | None:
        return self.workflow_config.queue

    def with_config(
        self, queue: str | HyrexQueue = None, priority: int = None
    ) -> "HyrexWorkflow":
        new_workflow_config = WorkflowConfig(
            config_phase=ConfigPhase.send, queue=queue, priority=priority
        )
        new_workflow = HyrexWorkflow(
            name=self.name,
            workflow_config=self.workflow_config.merge(new_workflow_config),
            workflow_arg_schema=self.workflow_arg_schema,
            workflow_builder=self.workflow_builder,
            dispatcher=self.dispatcher,
        )
        return new_workflow

    def serialize_workflow_to_task_requests(
        self,
        workflow_run_id: UUID,
    ) -> list[EnqueueTaskRequest]:
        node_to_task_request: dict[DagNode, EnqueueTaskRequest] = {}
        dependency_map: dict[DagNode, set[UUID]] = {}

        for node in self.workflow_builder.get_root_nodes():
            self.traverse(
                node=node,
                node_to_request=node_to_task_request,
                dependency_map=dependency_map,
                workflow_run_id=workflow_run_id,
            )

        for node, task_request in node_to_task_request.items():
            deps = dependency_map.get(node)
            if deps and len(deps) > 0:
                task_request.workflow_dependencies = list(deps)
            else:
                # Root nodes (no dependencies) should be queued immediately
                task_request.status = TaskStatus.queued

        return node_to_task_request.values()

    def send(self, context: BaseModel):
        # Runtime type checking to ensure context is the expected schema type
        if not isinstance(context, self.workflow_arg_schema):
            raise TypeError(
                f"Expected context of type {self.workflow_arg_schema.__name__}, "
                f"got {type(context).__name__} instead"
            )

        workflow_run_request = WorkflowRunRequest(
            id=uuid7(),
            workflow_name=self.name,
            args=context.model_dump(),
            queue=self.workflow_config.queue,
            timeout_seconds=self.workflow_config.timeout_seconds,
            idempotency_key=self.workflow_config.idempotency_key,
        )

        self.dispatcher.send_workflow_run(workflow_run_request)

    def traverse(
        self,
        node: DagNode,
        node_to_request: dict[DagNode, EnqueueTaskRequest],
        dependency_map: dict[DagNode, set[UUID]],
        workflow_run_id: UUID,
    ) -> None:
        """
        Recursively traverses the workflow DAG starting from the specified node and serializes each node into a task request.

        For each node, a unique UUID is generated which is used as the task's `id`, `durable_id`, and `root_id`.
        The workflow run ID is assigned to each task request, and for every child node, the parent's ID is added as a dependency.

        Args:
            node: The current workflow node to process.
            node_to_request: A map tracking nodes that have already been serialized along with their corresponding task requests.
            dependency_map: A map recording dependency edges, mapping each node to a set of parent task IDs (dependencies).
            workflow_run_id: The workflow run ID to assign to every task request.
        """
        # Create a new task request if the node hasn't been processed
        if node not in node_to_request:
            # Generate a single UUID for the task
            task_id = uuid7()

            # For max_retries, timeout_seconds, idempotency_key, etc.
            task_config = node.task_wrapper.task_config.apply_workflow_config(
                self.workflow_config
            )

            # All three IDs are set to the same UUID
            task_request = EnqueueTaskRequest(
                id=task_id,
                durable_id=task_id,
                root_id=task_id,
                workflow_run_id=workflow_run_id,
                workflow_dependencies=None,
                parent_id=None,
                status=TaskStatus.await_deps,
                task_name=node.task_wrapper.task_identifier,
                args={},
                queue=task_config.get_queue_name(),
                max_retries=task_config.max_retries,
                priority=task_config.priority,
                timeout_seconds=task_config.timeout_seconds,
                idempotency_key=task_config.idempotency_key,
            )

            node_to_request[node] = task_request

        current_request = node_to_request[node]

        # Process each child of the current node
        for child in node.get_children():
            # Record the dependency: the current task's id (a.k.a. durable_id) is a prerequisite for the child
            if child not in dependency_map:
                dependency_map[child] = set()
            dependency_map[child].add(current_request.id)

            # Recursively traverse the child node
            self.traverse(child, node_to_request, dependency_map, workflow_run_id)
