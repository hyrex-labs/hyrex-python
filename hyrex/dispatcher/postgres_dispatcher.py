import threading
from queue import Queue, Empty
from uuid import UUID
import time

from psycopg_pool import ConnectionPool

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

        self.thread = threading.Thread(target=self._batch_insert)
        self.thread.start()

    def enqueue(self, task: HyrexTask):
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
        with self.pool.connection() as conn:
            with conn.cursor() as cur:
                insert_query = "INSERT INTO tasks (data) VALUES %s"
                extras.execute_values(cur, insert_query, [(task,) for task in tasks])
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

    # def enqueue(self, task: HyrexTask):
    #     with self.pool.connection() as conn:
    #         with conn.cursor() as cur:
    #             cur.execute(
    #                 sql.ENQUEUE_TASK,
    #                 {
    #                     "id": task.id,
    #                     "root_id": task.root_id,
    #                     "task_name": task.task_name,
    #                     "args": task.args,
    #                     "queue": task.queue,
    #                     "max_retries": task.max_retries,
    #                     "priority": task.priority,
    #                 },
    #             )

    def dequeue(self, queue: str = constants.DEFAULT_QUEUE):
        pass

    def wait(self, task_id: UUID):
        pass
