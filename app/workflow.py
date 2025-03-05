import time
from typing import Literal

from pydantic import BaseModel

from hyrex.hyrex_context import get_hyrex_workflow_args
from hyrex.hyrex_registry import HyrexRegistry

hy = HyrexRegistry()


@hy.task
def initiate_onboard():
    args = OnboardUserWorkflowArg.model_validate(get_hyrex_workflow_args())
    print(args)
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


@hy.workflow(
    queue="onboard-user",
    timeout_seconds=100,
    workflow_arg_schema=OnboardUserWorkflowArg,
)
def onboard_user():
    (
        initiate_onboard
        >> [validate_payment, validate_identity, validate_org]
        >> approve_user
    )

    (
        validate_identity.with_config(priority=5)
        >> check_credit.with_config(queue="credit-queue")
        >> train_credit_machine_learning_model.with_config(queue="credit-queue")
    )


# onboard_user.with_config(queue="new_queue").send()
