import logging
import random
import time
from datetime import datetime

from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel

load_dotenv()

from .tasks import (
    EmptyContext,
    SleepContext,
    empty_task,
    error_task,
    print_random_number,
    root_level_task,
    sleepy_task,
    spawn_empty_tasks,
)
from .workflow import OnboardUserWorkflowArg, onboard_user

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
    empty_task.send()


@app.get("/spawn-empty-tasks/")
async def send_empty_tasks():
    # Test out type hints
    spawn_empty_tasks.send(50)


@app.get("/sleepy-task/")
async def run_sleepy_task(seconds: int):
    task = sleepy_task.with_config(timeout_seconds=5, max_retries=1).send(
        context=SleepContext(duration=seconds)
    )
    task.wait()
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
    # error_task.with_config(max_retries=10).send(EmptyContext())
    error_task.with_config(max_retries=5).send(context=EmptyContext())


@app.get("/random-number/")
async def random_number_task():
    task = print_random_number.send()
    # task.wait()
    # print(task.get_result())
    # return task.get_result()


@app.get("/root-level-task/")
async def send_root_level_task():
    root_level_task.send()


@app.get("/onboard-user/")
async def onboard_user_workflow():
    onboard_user.send(
        OnboardUserWorkflowArg(user_email="email@website.com", sign_up_tier="PRO")
    )
    # onboard_user.send()
