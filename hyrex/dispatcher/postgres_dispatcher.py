from uuid import UUID

from hyrex import constants
from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.models import HyrexTask


class PostgresDispatcher(Dispatcher):
    def __init__(self, conn: str):
        self.conn = conn

    def enqueue(self, task: HyrexTask):
        pass

    def dequeue(self, queue: str = constants.DEFAULT_QUEUE):
        pass

    def wait(self, task_id: UUID)
