from typing import Literal

from pydantic import BaseModel

from hyrex.hyrex_registry import HyrexRegistry

hy = HyrexRegistry()

@hy.task
def initiate_onboard():
    pass

@hy.task
def validate_payment():
    pass

@hy.task
def validate_identity():
    pass

@hy.task
def validate_org():
    pass

@hy.task
def approve_user():
    pass

@hy.task
def check_credit():
    pass

@hy.task
def train_credit_machine_learning_model():
    pass

class OnboardUserWorkflowArg(BaseModel):
    user_email: str
    sign_up_tier: Literal["FREE", "PRO", "ENTERPRISE"]

@hy.workflow(
    queue="onboard-user",
    workflowArgSchema=OnboardUserWorkflowArg,
)
def onboard_user(workflow_builder):
    workflow_builder >> initiate_onboard >> [validate_payment, validate_identity, validate_org] >> approve_user
    validate_identity >> check_credit >> train_credit_machine_learning_model

onboard_user.send(user_email="trevor@hyrex.io", sign_up_tier="PRO")
