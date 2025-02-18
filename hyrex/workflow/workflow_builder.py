import collections
import json
from typing import Sequence

# from hyrex.schemas import WorkflowDagJson
from hyrex.task import TaskWrapper


class DagNode:
    def __init__(self, task_wrapper: TaskWrapper):
        self.task_wrapper = task_wrapper
        self.children = []

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
        # Check if there's already a path from the new child back to self.
        # If so, adding child would create a cycle.
        if child.has_path_to(self):
            raise ValueError("Adding this child would create a cycle!")
        self.children.append(child)

    def send(self, *args, **kwargs):
        # TODO
        pass

    def __rshift__(
        self, other: "DagNode" | Sequence["DagNode"]
    ) -> "DagNode" | Sequence["DagNode"]:
        if isinstance(other, DagNode):
            self.add_child(other)
        elif isinstance(other, collections.abc.Sequence):
            for child in other:
                self.add_child(child)
        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

        return other  # Allows chaining like task1 >> task2 >> task3

    def __rrshift__(self, other: Sequence["DagNode"]) -> "DagNode":
        if not isinstance(other, collections.abc.Sequence):
            raise TypeError(f"Unknown type of {other}, {type(other)}")
        for parent in other:
            if not isinstance(parent, DagNode):
                raise TypeError(
                    f"Cannot use object {parent} of type {type(parent)} as node in DAG."
                )
            parent.add_child(self)

        return other  # Allows chaining like task1 >> task2 >> task3


class HyrexWorkflowBuilder:
    def __init__(
        self,
    ):
        self.root_nodes = []

    def __rshift__(
        self, other: TaskWrapper | Sequence[TaskWrapper]
    ) -> DagNode | list[DagNode]:
        if isinstance(other, TaskWrapper):
            new_node = DagNode(task_wrapper=other)
            self.root_nodes.append(new_node)
            return new_node

        elif isinstance(other, collections.abc.Sequence):
            new_nodes = [DagNode(child) for child in other]
            self.root_nodes += new_nodes
            return new_nodes

        else:
            raise TypeError(f"Unknown type of {other}, {type(other)}")

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

        for root in self.root_nodes:
            dfs(root)

        # TODO: Consider using a more specific schema
        return {"nodes": list(nodes_dict.values()), "edges": edges}
