import random
import time

from pydantic import BaseModel

from hyrex import HyrexQueue, HyrexRegistry, get_hyrex_context


def task_error_callback(task_name: str, e: Exception):
    print(f"{task_name} failed with exception: {str(e)}")


hy = HyrexRegistry()


class EmptyContext(BaseModel):
    pass


class SleepContext(BaseModel):
    duration: float


@hy.task(
    priority=2, queue=HyrexQueue(name="sleepy", concurrency_limit=1), max_retries=2
)
async def sleepy_task(context: SleepContext):
    time.sleep(context.duration)
    print("Time to wake up!")


@hy.task
def empty_task():
    print("Task complete.")


@hy.task
def spawn_empty_tasks(num: int):
    for _ in range(num):
        empty_task.send()


def error_handler():
    print("It's working a bit!")
    print(get_hyrex_context())


def backup_strategy(attempt_number: int):
    if attempt_number == 0:
        return 0
    else:
        return attempt_number * 10


@hy.task(max_retries=3, on_error=error_handler, retry_backoff=backup_strategy)
def error_task(context: EmptyContext):
    raise RuntimeError("The task has caused an error!")


# @hy.task(cron="* * * * *")
@hy.task
def print_random_number():
    random_number = random.random()
    print(random_number)
    return {"output": random_number}


@hy.task(queue="level3")
def level_three_task():
    context = get_hyrex_context()
    print(f"Level three task: {context}")
    time.sleep(0.003)


@hy.task(queue="level2")
def level_two_task():
    context = get_hyrex_context()
    print(f"Level two task: {context}")
    time.sleep(0.03)

    num_tasks = 20
    for _ in range(num_tasks):
        level_three_task.send()

    return {"tasks_queued": num_tasks}


@hy.task
def root_level_task():
    context = get_hyrex_context()
    print(f"Root level task: {context}")
    time.sleep(0.3)

    num_tasks = 5000
    for _ in range(num_tasks):
        level_two_task.send()

    return {"tasks_queued": num_tasks}
