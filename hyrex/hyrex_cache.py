from abc import ABC, abstractmethod
from typing import Any, Hashable, TypeVar

_cache_manager: dict[Hashable, "HyrexCache"] = {}


class HyrexCacheManager:
    @staticmethod
    def get(key: Hashable) -> "HyrexCache | None":
        return _cache_manager.get(key)

    @staticmethod
    def set(key: Hashable, value: "HyrexCache") -> None:
        _cache_manager[key] = value

    @staticmethod
    def cleanup() -> None:
        for val in _cache_manager.values():
            val.cleanup(val.cached_object)


# Define a TypeVar
T = TypeVar("T")


class HyrexCache(ABC):
    """Base class for cached resources in Hyrex"""

    def __init__(self):
        self.cached_object: T | None = None

    @classmethod
    def get(cls, hashable_key: Hashable) -> T:
        instance = HyrexCacheManager.get(hashable_key)
        if instance:
            return instance.cached_object

        # Create new instance and initialize
        instance = cls()
        instance.cached_object = cls.initialize(hashable_key)
        HyrexCacheManager.set(hashable_key, instance)
        return instance.cached_object

    @staticmethod
    @abstractmethod
    def initialize(key: Hashable):
        pass

    @staticmethod
    @abstractmethod
    def cleanup(cached_object: T) -> None:
        pass
