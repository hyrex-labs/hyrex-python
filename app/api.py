from fastapi import FastAPI
from pydantic import BaseModel
import random
from datetime import datetime
import logging

from .tasks import (
    sleepy_task,
    SleepContext,
    empty_task,
    EmptyContext,
    error_task,
)


app = FastAPI()
hyrex_logger = logging.getLogger("hyrex")
hyrex_logger.setLevel(logging.INFO)


@app.get("/")
async def root():
    return {}


# Kick off tasks with simple profiling
@app.get("/generate-tasks/")
async def generate_tasks(num_tasks: int):
    print("start:")
    print(datetime.now())
    for i in range(num_tasks):
        if i % 50 == 0:
            print(i)
            print(datetime.now())
        # empty_task.withConfig(queue=str(i)).send(EmptyContext())
        empty_task.send(EmptyContext())

    print("end:")
    print(datetime.now())


# @app.get("/generate-random-number/")
# async def generate_tasks():
#     random_number_task.send(RandomNumberContext())


@app.get("/empty-task/")
async def run_empty_task():
    empty_task.send(EmptyContext())


@app.get("/sleepy-task/")
async def run_sleepy_task(seconds: int):
    task = sleepy_task.withConfig(timeout_seconds=5).send(
        SleepContext(duration=seconds)
    )
    # tasks = []
    # for i in range(10):
    #     tasks.append(sleepy_task.send(SleepContext(duration=seconds)))
    # # task.wait()
    # tasks[5].cancel()
    # tasks[9].cancel()


def print_hello():
    print("HELLO")


@app.get("/error-task/")
async def run_error_task():
    error_task.withConfig(max_retries=1).send(EmptyContext())
