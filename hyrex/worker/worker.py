from typing import Any, Callable

from hyrex import constants
from hyrex.connection_hook import ConnectionHook
from hyrex.hyrex_registry import HyrexRegistry


class HyrexWorker:
    """
    Handles task registration and config for Hyrex worker process.
    """

    def __init__(
        self, queue: str = constants.ANY_QUEUE, error_callback: Callable = None
    ):
        self.queue = queue
        self.task_registry: HyrexRegistry = HyrexRegistry()
        self._connection_hooks: dict[str, ConnectionHook] = {}

    def add_registry(self, registry: HyrexRegistry):
        self.task_registry.add_registry(registry)

    def register_connection(self, hook: ConnectionHook) -> None:
        """Register a connection hook with this worker"""
        if hook.name in self._connection_hooks:
            raise ValueError(f"A connection hook is already registered with name: {hook.name}")
        self._connection_hooks[hook.name] = hook
        
    def get_connection(self, name: str) -> Any:
        """Get a connection by name"""
        hook = self._connection_hooks.get(name)
        if not hook:
            raise KeyError(f"No connection hook registered for: {name}")
        return hook.get_connection()
    
    def cleanup_connections(self) -> None:
        """Cleanup all connections"""
        for hook in self._connection_hooks.values():
            hook.cleanup()
        self._connection_hooks.clear()
