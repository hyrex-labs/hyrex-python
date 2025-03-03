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


@hy.task(priority=2, queue=HyrexQueue(name="sleepy", concurrency_limit=1))
async def sleepy_task(context: SleepContext):
    time.sleep(context.duration)
    print("Time to wake up!")


@hy.task
def empty_task():
    print("Task complete.")


def error_handler():
    print("It's working a bit!")
    print(get_hyrex_context())


@hy.task(max_retries=3, on_error=error_handler)
def error_task(context: EmptyContext):
    raise RuntimeError("The task has caused an error!")


# @hy.task(cron="* * * * *")
@hy.task
def print_random_number():
    random_number = random.random()
    print(random_number)
    return {"output": random_number}
    # print(random.random())
