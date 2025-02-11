from abc import ABC, abstractmethod
from typing import Any, Iterator

from .data import InMemoryData


class CacheScheduler(ABC):
    def __new__(cls):
        if not hasattr(cls, '__instance'):
            setattr(cls, '__instance', super().__new__(cls))
        return getattr(cls, '__instance')

    @abstractmethod
    def get_hook(self, repository: 'InMemoryCacheRepository', key: str, data: InMemoryData): ...

    @abstractmethod
    def set_hook(self, repository: 'InMemoryCacheRepository', key: str, data: InMemoryData): ...

    @abstractmethod
    def clear(self): ...


class DoNothingScheduler(CacheScheduler):
    def get_hook(self, repository: 'InMemoryCacheRepository', key: str, data: InMemoryData):
        pass

    def set_hook(self, repository: 'InMemoryCacheRepository', key: str, data: InMemoryData):
        pass

    def clear(self):
        pass


# TODO: ambiguous class name
class InstantScheduler(CacheScheduler):
    def get_hook(self, repository: 'InMemoryCacheRepository', key: str, data: InMemoryData):
        repository.remove(key)

    def set_hook(self, repository: 'InMemoryCacheRepository', key: str, data: InMemoryData):
        pass

    def clear(self):
        pass


class InMemoryCacheRepository:
    _cache: dict[str, InMemoryData] = {}
    _scheduler: CacheScheduler = DoNothingScheduler()

    def __new__(cls):
        if not hasattr(cls, '__instance'):
            cls.__instance = super().__new__(cls)
        return cls.__instance

    @classmethod
    def set_scheduler(cls, cache_scheduler: CacheScheduler):
        cls._scheduler = cache_scheduler

    def get_value(self, key: str) -> Any:
        data = self._get_data(key)
        self._scheduler.get_hook(self, key, data)
        return data.value

    def get_last_modification_time(self, key: str):
        return self._get_data(key).last_modification_time

    def _get_data(self, key: str) -> InMemoryData:
        return self._cache[key]

    def set_value(self, key: str, obj: Any) -> None:
        data = InMemoryData.create_data(obj)
        self._scheduler.set_hook(self, key, data)
        self._set_data(key, data)

    def _set_data(self, key: str, data: InMemoryData):
        self._cache[key] = data

    def has(self, key: str) -> bool:
        return key in self._cache

    def remove(self, key: str) -> None:
        assert self.has(key), f'{key} does not exist.'
        del self._cache[key]

    def empty(self) -> bool:
        return not self._cache

    @classmethod
    def clear(cls) -> None:
        cls._cache.clear()
        cls._scheduler.clear()

    def get_gen(self) -> Iterator[tuple[str, Any]]:
        for key, data in self._cache.items():
            yield key, data.value

    @property
    def size(self) -> int:
        return len(self._cache)

    @property
    def scheduler(self) -> CacheScheduler:
        return self._scheduler
