import random
import time
from typing import Literal

from pydantic import BaseModel

from hyrex.hyrex_context import get_hyrex_workflow_context
from hyrex.hyrex_registry import HyrexRegistry

hy = HyrexRegistry()


@hy.task
def initiate_onboard():
    # Uncomment to use workflow args
    # context = get_hyrex_workflow_context()
    # if context and context.workflow_run_args:
    #     args = OnboardUserWorkflowArg.model_validate(context.workflow_run_args)
    time.sleep(5)


@hy.task(max_retries=5)
def validate_payment():
    if random.random() < 0.5:
        raise Exception("Random exception occurred!")
    time.sleep(5)


@hy.task(max_retries=5)
def validate_identity():
    if random.random() < 0.5:
        raise Exception("Random exception occurred!")
    time.sleep(5)


@hy.task
def validate_org():
    context = get_hyrex_workflow_context()
    if context:
        # Get workflow arguments
        args = context.workflow_args

        # Get DurableTaskRun for a specific task
        payment_run = context.durable_runs.get("validate_payment")
        if payment_run:
            # Check task status
            payment_run.refresh()  # Get latest status
            for task_run in payment_run.task_runs:
                print(f"Payment task status: {task_run.status}")

        # Print the full context
        print(context)

    time.sleep(5)


@hy.task(max_retries=5)
def approve_user():
    if random.random() < 0.5:
        raise Exception("Random exception occurred!")
    time.sleep(5)


@hy.task(max_retries=5)
def check_credit():
    if random.random() < 0.5:
        raise Exception("Random exception occurred!")
    time.sleep(5)


@hy.task
def train_credit_machine_learning_model():
    time.sleep(5)


class OnboardUserWorkflowArg(BaseModel):
    user_email: str
    sign_up_tier: Literal["FREE", "PRO", "ENTERPRISE"]


@hy.workflow(
    queue="onboard-user",
    timeout_seconds=100,
    workflow_arg_schema=OnboardUserWorkflowArg,
    # cron="* * * * *",
)
def onboard_user():
    (
        initiate_onboard
        >> [validate_payment, validate_identity, validate_org]
        >> approve_user
    )

    (
        validate_identity
        >> check_credit.with_config(queue="credit-queue")
        >> train_credit_machine_learning_model.with_config(queue="credit-queue")
    )


@hy.task
def run_onboard_user_workflows(num: int):
    for _ in range(num):
        onboard_user.send(
            OnboardUserWorkflowArg(user_email="email@website.com", sign_up_tier="PRO")
        )
