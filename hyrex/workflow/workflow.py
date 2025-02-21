from typing import Generic, TypeVar
from uuid import UUID
from uuid_extensions import uuid7
from pydantic import BaseModel

from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.schemas import EnqueueTaskRequest, TaskStatus, WorkflowRunRequest
from hyrex.task_config import TaskConfig
from hyrex.workflow.workflow_builder import DagNode, WorkflowBuilder


T = TypeVar("T", bound=BaseModel)


class HyrexWorkflow(Generic[T]):
    def __init__(
        self,
        name: str,
        task_config: TaskConfig,
        workflow_arg_schema: T,
        workflow_builder: WorkflowBuilder,
        dispatcher: Dispatcher,
    ):
        self.name = name
        self.task_config = task_config
        self.workflow_arg_schema = workflow_arg_schema
        self.workflow_builder = workflow_builder
        self.dispatcher = dispatcher

    def withConfig(
        self,
    ):
        # TODO: Complete
        pass

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

    def send(self, context: T):
        # TODO: Fill in all other config fields. Maybe make a separate workflow config class.
        workflow_run_request = WorkflowRunRequest(
            id=uuid7(),
            workflow_name=self.name,
            args=context.model_dump(),
            queue=self.task_config.queue,
            timeout_seconds=self.task_config.timeout_seconds,
            idempotency_key=self.task_config.idempotency_key,
        )

        workflow_run_id = self.dispatcher.send_workflow_run(workflow_run_request)

        task_requests = self.serialize_workflow_to_task_requests(workflow_run_id)

        self.dispatcher.enqueue(task_requests)

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

            # TODO: Cleaner separation of task and workflow configurations.

            # For max_retries, timeout_seconds, idempotency_key, etc.
            task_config = node.task_wrapper.task_config
            # Merge with workflow config for queue, priority, etc.
            merged_config = task_config.merge(self.task_config)

            # All three IDs are set to the same UUID
            task_request = EnqueueTaskRequest(
                id=task_id,
                durable_id=task_id,
                root_id=task_id,
                workflow_run_id=workflow_run_id,
                workflow_dependencies=None,
                parent_id=None,
                status=TaskStatus.waiting,
                task_name=node.task_wrapper.task_identifier,
                args={},
                queue=merged_config.get_queue_name(),
                max_retries=task_config.max_retries,
                priority=merged_config.priority,
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
