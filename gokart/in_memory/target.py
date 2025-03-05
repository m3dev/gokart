from __future__ import annotations

from datetime import datetime
from typing import Any

from gokart.in_memory.repository import InMemoryCacheRepository
from gokart.target import TargetOnKart, TaskLockParams

_repository = InMemoryCacheRepository()


class InMemoryTarget(TargetOnKart):
    def __init__(self, data_key: str, task_lock_param: TaskLockParams):
        if task_lock_param.should_task_lock:
            raise ValueError('Redis with `InMemoryTarget` is not currently supported.')

        self._data_key = data_key
        self._task_lock_params = task_lock_param

    def _exists(self) -> bool:
        return _repository.has(self._data_key)

    def _get_task_lock_params(self) -> TaskLockParams:
        return self._task_lock_params

    def _load(self) -> Any:
        return _repository.get_value(self._data_key)

    def _dump(
        self,
        obj: Any,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, Any] | None = None,
        required_task_outputs: dict[str, str] | None = None,
    ) -> None:
        return _repository.set_value(self._data_key, obj)

    def _remove(self) -> None:
        _repository.remove(self._data_key)

    def _last_modification_time(self) -> datetime:
        if not _repository.has(self._data_key):
            raise ValueError(f'No object(s) which id is {self._data_key} are stored before.')
        time = _repository.get_last_modification_time(self._data_key)
        return time

    def _path(self) -> str:
        # TODO: this module name `_path` migit not be appropriate
        return self._data_key


def make_in_memory_target(target_key: str, task_lock_params: TaskLockParams) -> InMemoryTarget:
    return InMemoryTarget(target_key, task_lock_params)
