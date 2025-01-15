from abc import ABC, abstractmethod
from typing import Any, Hashable, TypeVar

class HyrexCacheManager(dict[Hashable, "HyrexCache"]):
    def __setitem__(self, key, value):
        # Override [] assignment
        if not isinstance(key, Hashable):
            raise TypeError(f"Key must be hashable, got {type(key)}")
        if not isinstance(value, HyrexCache):
            raise TypeError(f"Value must be HyrexCache, got {type(key)}")
        super().__setitem__(key, value)

    def cleanup():
        pass


# Define a TypeVar
T = TypeVar("T")


class HyrexCache(ABC):
    """Base class for cached resources in Hyrex"""
    cached_object = None

    def get(self, hashableKey: Hashable) -> T:
        cached = HyrexCacheManager.get(hashableKey)
        if cached:
            return cached.cached_object
        else:
            self.cached_object = self.initialize(hashableKey)
        HyrexCacheManager[hashableKey] = self
        return self.cached_object

    @staticmethod
    @abstractmethod
    def initialize(hashableKey: Hashable):
        pass

    @staticmethod
    @abstractmethod
    def cleanup(cached_object: T) -> None:
        pass
