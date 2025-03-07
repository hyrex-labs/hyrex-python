import collections
import json
from typing import Sequence

from hyrex.task_wrapper import TaskWrapper
from hyrex.workflow.workflow_builder_context import (
    clear_current_workflow_builder, set_current_workflow_builder)


class DagNode:
    def __init__(self, task_wrapper: TaskWrapper, workflow_builder: "WorkflowBuilder"):
        self.task_wrapper = task_wrapper
        self.workflow_builder = workflow_builder
        self.children = []
        self.parents = []

    def has_path_to(self, target: "DagNode", visited=None) -> bool:
        if visited is None:
            visited = set()
        if self is target:
            return True
        visited.add(self)
        for child in self.children:
            if child not in visited and child.has_path_to(target, visited):
                return True
        return False

    def add_child(self, child: "DagNode"):
        if child.has_path_to(self):
            raise ValueError("Adding this child would create a cycle!")
        if child in self.children:
            raise ValueError("This node is already a child of mine!")

        self.children.append(child)
        child.parents.append(self)

    def get_children(self) -> list["DagNode"]:
        return self.children

    def __rshift__(self, other: TaskWrapper | Sequence[TaskWrapper] | "DagNode"):
        if isinstance(other, TaskWrapper):
            node = self.workflow_builder.get_or_create_node(other)
            self.add_child(node)
            return node
        elif isinstance(other, collections.abc.Sequence):
            nodes = [
                (
                    self.workflow_builder.get_or_create_node(task)
                    if isinstance(task, TaskWrapper)
                    else task
                )
                for task in other
            ]
            for node in nodes:
                self.add_child(node)
            return nodes
        elif isinstance(other, DagNode):
            self.add_child(other)
            return other
        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

    def __rrshift__(self, other: Sequence["TaskWrapper | DagNode"]) -> "DagNode":
        if not isinstance(other, collections.abc.Sequence):
            raise TypeError(f"Unknown type of {other}, {type(other)}")

        for item in other:
            if isinstance(item, TaskWrapper):
                node = self.workflow_builder.get_or_create_node(item)
                node.add_child(self)
            elif isinstance(item, DagNode):
                item.add_child(self)
            else:
                raise TypeError(
                    f"Cannot use object {item} of type {type(item)} as node in DAG"
                )

        return self  # Return self to allow chaining like task1 >> task2 >> task3


class WorkflowBuilder:
    def __init__(self):
        self.nodes = {}  # Map from TaskWrapper to DagNode

    def __enter__(self) -> "WorkflowBuilder":
        set_current_workflow_builder(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        clear_current_workflow_builder()

    def get_or_create_node(self, task_wrapper: TaskWrapper) -> DagNode:
        if task_wrapper not in self.nodes:
            self.nodes[task_wrapper] = DagNode(
                task_wrapper=task_wrapper, workflow_builder=self
            )
        return self.nodes[task_wrapper]

    def get_root_nodes(self):
        return [node for node in self.nodes.values() if not node.parents]

    def to_json(self) -> dict:
        """
        Returns a JSON dict representing the workflow DAG.
        The JSON has the format:
        {
          "nodes": [
            { "id": "1", "name": "TaskIdentifier1" },
            { "id": "2", "name": "TaskIdentifier2" },
            ...
          ],
          "edges": [
            { "from": "1", "to": "2" },
            { "from": "1", "to": "3" },
            ...
          ]
        }
        """
        nodes_dict = {}
        edges = []
        visited = set()
        node_ids = {}  # Maps node (via id(node)) to a unique increasing integer.
        next_id = [1]  # Using a list to allow modification in nested scope.

        def dfs(node: DagNode):
            node_key = id(node)
            if node_key in visited:
                return
            visited.add(node_key)

            # Assign a unique increasing id to the node if not already assigned.
            if node_key not in node_ids:
                node_ids[node_key] = next_id[0]
                next_id[0] += 1

            # Use task_wrapper.task_identifier as the node's name.
            node_name = getattr(
                node.task_wrapper, "task_identifier", f"node_{node_ids[node_key]}"
            )
            nodes_dict[node_ids[node_key]] = {
                "id": str(node_ids[node_key]),
                "name": node_name,
            }

            for child in node.children:
                child_key = id(child)
                if child_key not in node_ids:
                    node_ids[child_key] = next_id[0]
                    next_id[0] += 1
                edges.append(
                    {"from": str(node_ids[node_key]), "to": str(node_ids[child_key])}
                )
                dfs(child)

        for root in self.get_root_nodes():
            dfs(root)

        # TODO: Consider using a more specific schema
        return {"nodes": list(nodes_dict.values()), "edges": edges}
