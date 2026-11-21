from __future__ import annotations

from collections.abc import Generator, Iterator
from contextlib import contextmanager
from datetime import datetime
from typing import Any

from .data import InMemoryData


class InMemoryCacheRepository:
    def __init__(self) -> None:
        self._cache: dict[str, InMemoryData] = {}

    def get_value(self, key: str) -> Any:
        # WHY: the dumped object itself is returned without copying, to avoid the cost of copying large data.
        return self._get_data(key).value

    def get_last_modification_time(self, key: str) -> datetime:
        return self._get_data(key).last_modification_time

    def _get_data(self, key: str) -> InMemoryData:
        return self._cache[key]

    def set_value(self, key: str, obj: Any) -> None:
        data = InMemoryData.create_data(obj)
        self._cache[key] = data

    def has(self, key: str) -> bool:
        return key in self._cache

    def remove(self, key: str) -> None:
        assert self.has(key), f'{key} does not exist.'
        del self._cache[key]

    def empty(self) -> bool:
        return not self._cache

    def clear(self) -> None:
        self._cache.clear()

    def get_gen(self) -> Iterator[tuple[str, Any]]:
        for key, data in self._cache.items():
            yield key, data.value

    @property
    def size(self) -> int:
        return len(self._cache)


# WHY: a module variable instead of a ContextVar, so that threads started in a task's run() see the same repository.
_current_repository: InMemoryCacheRepository | None = None


def get_current_repository() -> InMemoryCacheRepository | None:
    return _current_repository


@contextmanager
def in_memory_scope() -> Generator[InMemoryCacheRepository]:
    """Provide an empty repository for in-memory targets while the context is active, and drop it on exit."""
    global _current_repository
    outer = _current_repository
    _current_repository = InMemoryCacheRepository()
    try:
        yield _current_repository
    finally:
        _current_repository = outer
