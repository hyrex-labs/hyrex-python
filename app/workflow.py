import time
from typing import Literal

from pydantic import BaseModel

from hyrex.hyrex_registry import HyrexRegistry

hy = HyrexRegistry()


@hy.task
def initiate_onboard():
    time.sleep(5)


@hy.task
def validate_payment():
    time.sleep(5)


@hy.task
def validate_identity():
    time.sleep(5)


@hy.task
def validate_org():
    time.sleep(5)


@hy.task
def approve_user():
    time.sleep(5)


@hy.task
def check_credit():
    time.sleep(5)


@hy.task
def train_credit_machine_learning_model():
    time.sleep(5)


class OnboardUserWorkflowArg(BaseModel):
    user_email: str
    sign_up_tier: Literal["FREE", "PRO", "ENTERPRISE"]


class NoContext(BaseModel):
    pass


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
