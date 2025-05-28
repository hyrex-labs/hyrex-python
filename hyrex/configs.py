import sys
from enum import Enum
from typing import Union

# Python 3.9 compatibility - StrEnum was introduced in Python 3.11
if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    class StrEnum(str, Enum):
        """Compatibility shim for Python < 3.11"""
        pass

from pydantic import BaseModel, Field, field_validator

from hyrex.hyrex_queue import HyrexQueue


class ConfigPhase(StrEnum):
    # Config set on HyrexRegistry
    registry = "registry"
    # Config set on decorator
    decorator = "decorator"
    # Config set at send time
    send = "send"


class WorkflowConfig(BaseModel):
    config_phase: ConfigPhase

    queue: Union[str, HyrexQueue, None] = None  # Overrides task queue
    priority: Union[int, None] = Field(default=None, ge=1, le=10)  # Overrides task priority

    timeout_seconds: Union[int, None] = Field(default=None, gt=0)  # For workflow
    idempotency_key: Union[str, None] = None  # For workflow

    @field_validator("config_phase")
    @classmethod  # This is optional in Pydantic v2
    def validate_username(cls, value):
        if value == ConfigPhase.registry:
            raise ValueError(
                "WorkflowConfigs should not be defined at the registry level."
            )
        return value

    def get_default_config(self):
        assert (
            self.config_phase == ConfigPhase.decorator
        ), "Default config must be specified at decorator-time."
        default_config = {}
        if self.queue:
            default_config["queue"] = self.get_queue_name()
        if self.priority:
            default_config["priority"] = self.priority
        if self.timeout_seconds:
            default_config["timeout_seconds"] = self.timeout_seconds
        return default_config

    def get_queue_name(self) -> str:
        if isinstance(self.queue, str):
            return self.queue
        else:
            return self.queue.name

    def merge(self, other: "WorkflowConfig") -> "WorkflowConfig":
        """Merge another config into this one, with other taking precedence"""
        if self.config_phase == ConfigPhase.send:
            assert other.config_phase != ConfigPhase.decorator

        merged_data = self.model_dump()
        other_data = other.model_dump()

        for field, value in other_data.items():
            if value is not None:
                merged_data[field] = value

        return WorkflowConfig(**merged_data)


class TaskConfig(BaseModel):
    config_phase: ConfigPhase

    queue: Union[str, HyrexQueue, None] = None
    priority: Union[int, None] = Field(default=None, ge=1, le=10)
    max_retries: Union[int, None] = Field(default=None, ge=0)
    timeout_seconds: Union[int, None] = Field(default=None, gt=0)
    idempotency_key: Union[str, None] = None

    def get_default_config(self):
        assert (
            self.config_phase == ConfigPhase.decorator
        ), "Default config must be specified at decorator-time."
        default_config = {}
        if self.queue:
            default_config["queue"] = self.get_queue_name()
        if self.priority:
            default_config["priority"] = self.priority
        if self.max_retries:
            default_config["max_retries"] = self.max_retries
        if self.timeout_seconds:
            default_config["timeout_seconds"] = self.timeout_seconds
        return default_config

    def get_queue_name(self) -> str:
        if isinstance(self.queue, str):
            return self.queue
        else:
            return self.queue.name

    def merge(self, other: "TaskConfig") -> "TaskConfig":
        """Merge another config into this one, with other taking precedence"""
        if self.config_phase == ConfigPhase.decorator:
            assert other.config_phase != ConfigPhase.registry
        elif self.config_phase == ConfigPhase.send:
            assert other.config_phase not in [
                ConfigPhase.decorator,
                ConfigPhase.registry,
            ]

        merged_data = self.model_dump()
        other_data = other.model_dump()

        for field, value in other_data.items():
            if value is not None:
                merged_data[field] = value

        return TaskConfig(**merged_data)

    def apply_workflow_config(self, workflow_config: WorkflowConfig) -> "TaskConfig":
        """Apply workflow config settings to this task config"""
        if workflow_config is None:
            return self

        updated_config = self.model_copy()

        if workflow_config.queue is not None:
            updated_config.queue = workflow_config.queue
        if workflow_config.priority is not None:
            updated_config.priority = workflow_config.priority

        return updated_config
