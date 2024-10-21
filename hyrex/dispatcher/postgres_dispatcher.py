import threading
import time
from queue import Empty, Queue
from uuid import UUID

from psycopg.types.json import Json
from psycopg_pool import ConnectionPool
from pydantic import BaseModel

from hyrex import constants, sql
from hyrex.dispatcher.dispatcher import Dispatcher
from hyrex.models import HyrexTask


class PostgresDispatcher(Dispatcher):
    def __init__(self, conn_string: str, batch_size=100, flush_interval=0.1):
        self.conn_string = conn_string
        self.pool = ConnectionPool(conn_string)

        self.queue = Queue()
        self.running = True
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        self.thread = threading.Thread(target=self._batch_enqueue)
        self.thread.start()

    def enqueue(
        self,
        task: HyrexTask,
    ):
        """
        Adds a task to the dispatcher's queue.

        :param task: The task to be enqueued.
        """
        self.queue.put(task)

    def _batch_enqueue(self):
        tasks = []
        last_flush_time = time.monotonic()
        while self.running or not self.queue.empty():
            timeout = self.flush_interval - (time.monotonic() - last_flush_time)
            if timeout <= 0:
                # Flush if the flush interval has passed
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()
                continue

            try:
                # Wait for a task or until the timeout expires
                task = self.queue.get(timeout=timeout)
                tasks.append(task)
                if len(tasks) >= self.batch_size:
                    # Flush if batch size is reached
                    self._enqueue_tasks(tasks)
                    tasks = []
                    last_flush_time = time.monotonic()
            except Empty:
                # No task received within the timeout
                if tasks:
                    self._enqueue_tasks(tasks)
                    tasks = []
                last_flush_time = time.monotonic()

        # Flush any remaining tasks when stopping
        if tasks:
            self._enqueue_tasks(tasks)

    def _enqueue_tasks(self, tasks):
        """
        Inserts a batch of tasks into the database.

        :param tasks: List of tasks to insert.
        """
        task_data = [
            (
                task.id,
                task.root_id,
                task.task_name,
                Json(task.args),
                task.queue,
                task.max_retries,
                task.priority,
            )
            for task in tasks
        ]

        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    sql.ENQUEUE_TASKS,
                    task_data,
                )
                conn.commit()

    def stop(self):
        """
        Stops the batching process and flushes remaining tasks.
        """
        self.running = False
        self.thread.join()
        # Close the connection pool
        self.pool.close()

    def __enter__(self):
        """
        Enter the runtime context related to this object.
        """
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context and ensure resources are cleaned up.
        """
        self.stop()

    def dequeue(self, queue: str = constants.DEFAULT_QUEUE):
        pass

    def wait(self, task_id: UUID):
        pass

    def retrieve_status(self, task_id: UUID):
        pass
