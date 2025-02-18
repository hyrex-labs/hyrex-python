from pydantic import BaseModel

from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.task_config import TaskConfig


class HyrexWorkflow:
    def __init__(
        self,
        name: str,
        task_config: TaskConfig,
        workflow_arg_schema: BaseModel,
        workflow_dag_json: dict,
        dispatcher: Dispatcher,
    ):
        self.name = name
        self.task_config = task_config
        self.workflow_arg_schema = workflow_arg_schema
        self.workflow_dag_json = workflow_dag_json
        self.dispatcher = dispatcher

    def send(self, *args, **kwargs):
        print(f"Sending... {self.workflow_identifier}")
