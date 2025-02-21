import time
from typing import Literal

from pydantic import BaseModel

from hyrex.hyrex_registry import HyrexRegistry

hy = HyrexRegistry()


class NoContext(BaseModel):
    pass


@hy.task
def initiate_onboard(context: NoContext):
    time.sleep(5)


@hy.task
def validate_payment(context: NoContext):
    time.sleep(5)


@hy.task
def validate_identity(context: NoContext):
    time.sleep(5)


@hy.task
def validate_org(context: NoContext):
    time.sleep(5)


@hy.task
def approve_user(context: NoContext):
    time.sleep(5)


@hy.task
def check_credit(context: NoContext):
    time.sleep(5)


@hy.task
def train_credit_machine_learning_model(context: NoContext):
    time.sleep(5)


class OnboardUserWorkflowArg(BaseModel):
    user_email: str
    sign_up_tier: Literal["FREE", "PRO", "ENTERPRISE"]


@hy.workflow(
    name="onboard-user",
    queue="onboard-user",
    workflow_arg_schema=NoContext,
)
def onboard_user():
    (
        initiate_onboard
        >> [validate_payment, validate_identity, validate_org]
        >> approve_user
    )

    validate_identity >> check_credit >> train_credit_machine_learning_model
