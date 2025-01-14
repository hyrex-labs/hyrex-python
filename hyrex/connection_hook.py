from abc import ABC, abstractmethod
from typing import Any, Dict


class ConnectionHook(ABC):
    """Base class for connection hooks"""

    def __init__(self):
        self._connection = None

    @property
    def name(self) -> str:
        """Unique identifier for this connection type"""
        return self.__class__.__name__

    @abstractmethod
    def create_connection(self) -> Any:
        """Create a new connection"""
        pass

    @abstractmethod
    def close_connection(self, connection: Any) -> None:
        """Close the given connection"""
        pass

    def get_connection(self) -> Any:
        """Get the cached connection, creating it if necessary"""
        if self._connection is None:
            self._connection = self.create_connection()
        return self._connection

    def cleanup(self) -> None:
        """Internal method to clean up the current connection"""
        if self._connection is not None:
            self.close_connection(self._connection)
            self._connection = None

    def health_check(self) -> bool:
        """Verify the connection is still viable"""
        return True
