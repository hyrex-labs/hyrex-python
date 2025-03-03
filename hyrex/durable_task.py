from uuid import UUID
from hyrex.dispatcher.dispatcher import Dispatcher


class DurableTask:
    def __init__(
        self,
        task_name: str,
        durable_id: UUID,
        dispatcher: Dispatcher,
    ):
        self.task_name = task_name
        self.durable_id = durable_id
        self.dispatcher = dispatcher

        self.task_runs = []

    def refresh(self):
        # TODO
        pass

    def wait(self):
        # TODO
        pass

    def get_result(self):
        # TODO
        pass
