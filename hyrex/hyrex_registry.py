import inspect
import logging
import os
from typing import Callable, overload

from hyrex import constants
from hyrex.configs import ConfigPhase, TaskConfig, WorkflowConfig
from hyrex.dispatcher import Dispatcher, get_dispatcher
from hyrex.env_vars import EnvVars
from hyrex.hyrex_queue import HyrexQueue
from hyrex.task_wrapper import P, R, TaskWrapper
from hyrex.workflow.workflow import HyrexWorkflow
from hyrex.workflow.workflow_builder import WorkflowBuilder


class HyrexRegistry:
    def __init__(
        self,
        queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
        max_retries: int = 0,
        priority: int = constants.DEFAULT_PRIORITY,
    ):
        self.logger = logging.getLogger(__name__)

        if os.getenv(EnvVars.WORKER_PROCESS):
            self.is_worker_process = True
        else:
            self.is_worker_process = False
        self.dispatcher = get_dispatcher(self.is_worker_process)

        self._task_registry: dict[str, TaskWrapper] = {}
        self._queue_registry: dict[str, HyrexQueue] = {}
        self._workflow_registry: dict[str, HyrexWorkflow] = {}

        # Registry-level task config.
        # Decorated tasks will merge their own configs into this.
        task_config = TaskConfig(
            config_phase=ConfigPhase.registry,
            queue=queue,
            max_retries=max_retries,
            priority=priority,
        )
        self.task_config = task_config

    def register_all_with_db(self):
        # Register tasks and workflows with DB
        for task in self._task_registry.values():
            self.dispatcher.register_task_def(
                task_name=task.task_identifier,
                arg_schema=task.get_arg_schema(),
                task_config=task.task_config,
                cron=task.cron,
                source_code=inspect.getsource(task.func),
            )

        for workflow in self._workflow_registry.values():
            self.dispatcher.register_workflow(
                name=workflow.name,
                source_code=workflow.source_code,
                workflow_dag_json=workflow.workflow_builder.to_json(),
                workflow_arg_schema=workflow.workflow_arg_schema,
                default_config=workflow.workflow_config.get_default_config(),
                cron=workflow.cron,
            )

    def register_task_def(self, task_wrapper: TaskWrapper):
        self.logger.debug(f"Registering task def: {task_wrapper.task_identifier}")
        if self._task_registry.get(task_wrapper.task_identifier):
            raise KeyError(
                f"Task {task_wrapper.task_identifier} is already registered. Task names must be unique."
            )
        self._task_registry[task_wrapper.task_identifier] = task_wrapper

        # Register the task wrapper's queue for tracking concurrency.
        queue = task_wrapper.get_queue()
        if isinstance(queue, str):
            self.register_queue(HyrexQueue(name=queue))
        else:
            self.register_queue(queue)

    def register_queue(self, queue: HyrexQueue | str):
        if self._queue_registry.get(queue.name) and not queue.equals(
            self._queue_registry[queue.name]
        ):
            raise KeyError(
                f"Conflicting concurrency limits on queue name: {queue.name}"
            )

        self._queue_registry[queue.name] = queue

    def register_workflow(self, workflow: HyrexWorkflow):
        self.logger.debug(f"Registering workflow: {workflow.name}")
        if self._workflow_registry.get(workflow.name):
            raise KeyError(
                f"Workflow {workflow.name} is already registered. Workflow names must be unique."
            )
        self._workflow_registry[workflow.name] = workflow

        # Register the workflow's queue for tracking concurrency.
        queue = workflow.get_queue()
        if not queue:
            return

        if isinstance(queue, str):
            self.register_queue(HyrexQueue(name=queue))
        else:
            self.register_queue(queue)

    def get_concurrency_limit(self, queue_name: str):
        if self._queue_registry.get(queue_name):
            return self._queue_registry[queue_name].concurrency_limit
        else:
            return 0

    def get_on_error_handler(self, task_name: str) -> Callable | None:
        task_wrapper = self._task_registry[task_name]
        return task_wrapper.on_error

    def get_retry_backoff(self, task_name: str, attempt_number: int) -> int:
        task_wrapper = self._task_registry[task_name]
        return task_wrapper.get_retry_backoff(attempt_number=attempt_number)

    def get_task_wrappers(self) -> list[TaskWrapper]:
        return self._task_registry.values()

    def get_task_names(self) -> list[str]:
        return [task.task_identifier for task in self._task_registry.values()]

    def get_workflows(self) -> list[HyrexWorkflow]:
        return self._workflow_registry.values()

    def get_task(self, task_name: str):
        return self._task_registry[task_name]

    def add_registry(self, registry: "HyrexRegistry"):
        for task_wrapper in registry.get_task_wrappers():
            self.register_task_def(task_wrapper=task_wrapper)
        for workflow in registry.get_workflows():
            self.register_workflow(workflow=workflow)

    @overload
    def task(self, func: Callable[P, R]) -> TaskWrapper[P, R]: ...

    @overload
    def task(
        self,
        func: None = None,
        *,
        queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
        cron: str | None = None,
        max_retries: int = 0,
        timeout_seconds: int | None = None,
        priority: int = constants.DEFAULT_PRIORITY,
        on_error: Callable | None = None,
        retry_backoff: int | Callable[[int], int] | None = None,
    ) -> Callable[[Callable[P, R]], TaskWrapper[P, R]]: ...

    def task(
        self,
        func: Callable[P, R] | None = None,
        *,
        queue: str | HyrexQueue = constants.DEFAULT_QUEUE,
        cron: str | None = None,
        max_retries: int = 0,
        timeout_seconds: int | None = None,
        priority: int = constants.DEFAULT_PRIORITY,
        on_error: Callable | None = None,
        retry_backoff: int | Callable[[int], int] | None = None,
    ) -> TaskWrapper[P, R] | Callable[[Callable[P, R]], TaskWrapper[P, R]]:
        """
        Create task decorator
        """

        def decorator(func: Callable[P, R]) -> TaskWrapper[P, R]:
            task_identifier = func.__name__
            decorated_task_config = TaskConfig(
                config_phase=ConfigPhase.decorator,
                queue=queue,
                max_retries=max_retries,
                timeout_seconds=timeout_seconds,
                priority=priority,
            )

            task_wrapper = TaskWrapper(
                task_identifier=task_identifier,
                func=func,
                cron=cron,
                task_config=self.task_config.merge(decorated_task_config),
                dispatcher=self.dispatcher,
                on_error=on_error,
                retry_backoff=retry_backoff,
            )
            # Register task within this registry
            self.register_task_def(task_wrapper=task_wrapper)
            return task_wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def workflow(
        self,
        queue: str | HyrexQueue = None,
        timeout_seconds: int | None = None,
        priority: int = None,
        cron: str = None,
        workflow_arg_schema=None,
    ):
        """
        A decorator to register a workflow.
        """

        def decorator(func):
            with WorkflowBuilder() as workflow_builder:
                # Build the workflow by calling the function
                func()

                # Use function name as workflow name
                workflow_name = func.__name__

                # Compile config object
                workflow_config = WorkflowConfig(
                    config_phase=ConfigPhase.decorator, queue=queue, priority=priority
                )

                # Create and return a HyrexWorkflow instance
                workflow = HyrexWorkflow(
                    name=workflow_name,
                    workflow_config=workflow_config,
                    workflow_arg_schema=workflow_arg_schema,
                    workflow_builder=workflow_builder,
                    dispatcher=self.dispatcher,
                    source_code=inspect.getsource(func),
                    cron=cron,
                )
                # Register workflow within this registry
                self.register_workflow(workflow)
            return workflow

        return decorator
