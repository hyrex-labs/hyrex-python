import sys
from enum import Enum
from typing import Union
from uuid import UUID

# Python 3.9 compatibility - StrEnum was introduced in Python 3.11
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    class StrEnum(str, Enum):
        """Compatibility shim for Python < 3.11"""
        pass

from pydantic import BaseModel


class CancelTaskMessage(BaseModel):
    task_id: UUID


class SetExecutorTaskMessage(BaseModel):
    executor_id: UUID
    task_id: Union[UUID, None]


class TaskRegistrationComplete(BaseModel):
    pass


class HeartbeatRequestMessage(BaseModel):
    pass
