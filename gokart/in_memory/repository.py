from __future__ import annotations

import threading
from collections.abc import Generator, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pandas as pd

from .data import InMemoryData


def _shallow_copy_data_frame(value: Any) -> Any:
    if isinstance(value, pd.DataFrame):
        # WHY: copying at both dump and load keeps changes made by the dumping task after dump, or by a loading task, from leaking
        # to other tasks, like a file output would. A shallow copy is cheap, and Copy-on-Write (always on since pandas 3) protects the values.
        # Other objects are stored and returned as is to avoid copying large data.
        return value.copy(deep=False)
    return value


class InMemoryCacheRepository:
    def __init__(self) -> None:
        self._cache: dict[str, InMemoryData] = {}

    def get_value(self, key: str) -> Any:
        return _shallow_copy_data_frame(self._get_data(key).value)

    def get_last_modification_time(self, key: str) -> datetime:
        return self._get_data(key).last_modification_time

    def _get_data(self, key: str) -> InMemoryData:
        return self._cache[key]

    def set_value(self, key: str, obj: Any) -> None:
        data = InMemoryData.create_data(_shallow_copy_data_frame(obj))
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


class InMemoryTargetError(Exception):
    """Raised when InMemoryTarget is used in an unsupported way."""


@dataclass
class _BuildScope:
    repository: InMemoryCacheRepository | None = None
    outer_repository: InMemoryCacheRepository | None = None
    outer_owner: int | None = None


# WHY: a module variable instead of a ContextVar, so that threads started in a task's run() see the same repository.
# Only the thread that runs the gokart.build owning it may replace it (see acquire_repository).
_current_repository: InMemoryCacheRepository | None = None
_current_owner: int | None = None
_lock = threading.Lock()


class _BuildScopes(threading.local):
    """The gokart.build calls running in the current thread, innermost last."""

    def __init__(self) -> None:
        self.stack: list[_BuildScope] = []


_build_scopes = _BuildScopes()


def get_current_repository() -> InMemoryCacheRepository | None:
    """Return the repository of the innermost gokart.build in this thread, or, in a thread without a build (e.g. one started in run()),
    the repository of the build that owns it."""
    stack = _build_scopes.stack
    if stack:
        return stack[-1].repository
    return _current_repository


@contextmanager
def in_memory_scope() -> Generator[None]:
    """Mark the current thread as running a gokart.build. The repository is created by acquire_repository() and dropped on exit."""
    global _current_repository, _current_owner
    scope = _BuildScope()
    stack = _build_scopes.stack
    stack.append(scope)
    try:
        yield
    finally:
        stack.pop()
        if scope.repository is not None:
            with _lock:
                _current_repository, _current_owner = scope.outer_repository, scope.outer_owner


def acquire_repository() -> InMemoryCacheRepository:
    """Return the repository of the innermost gokart.build in the current thread, creating it on first use.

    The repository is created only when the build has an in-memory output, so that builds without one never touch the shared
    repository and can run in other threads.
    """
    global _current_repository, _current_owner
    stack = _build_scopes.stack
    if not stack:
        raise InMemoryTargetError('InMemoryTarget can only be used inside gokart.build.')
    scope = stack[-1]
    if scope.repository is not None:
        return scope.repository
    with _lock:
        if _current_repository is not None and _current_owner != threading.get_ident():
            raise InMemoryTargetError(
                'Another thread is running gokart.build with in-memory outputs. '
                'Running gokart.build with in-memory outputs in multiple threads at the same time is not supported.'
            )
        scope.repository = InMemoryCacheRepository()
        scope.outer_repository, scope.outer_owner = _current_repository, _current_owner
        _current_repository, _current_owner = scope.repository, threading.get_ident()
    return scope.repository
