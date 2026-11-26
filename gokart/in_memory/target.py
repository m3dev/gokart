from __future__ import annotations

from datetime import datetime
from typing import Any

from gokart.in_memory.repository import InMemoryCacheRepository, InMemoryTargetError, find_repository
from gokart.required_task_output import RequiredTaskOutput
from gokart.target import TargetOnKart, _make_file_path  # pyright: ignore[reportPrivateUsage]
from gokart.utils import FlattenableItems


class InMemoryTarget(TargetOnKart):
    def __init__(self, data_key: str):
        self._data_key = data_key

    def _repository(self, write: bool = False) -> InMemoryCacheRepository:
        repository = find_repository(self._data_key, write=write)
        if repository is None:
            raise InMemoryTargetError(f'InMemoryTarget({self._data_key}) can only be used inside gokart.build.')
        return repository

    def _exists(self) -> bool:
        # WHY: complete() is also called outside gokart.build (e.g. task tree tools), where nothing is stored yet.
        repository = find_repository(self._data_key, write=False)
        return repository is not None and repository.has(self._data_key)

    def _load(self) -> Any:
        return self._repository().get_value(self._data_key)

    def _dump(
        self,
        obj: Any,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> None:
        return self._repository(write=True).set_value(self._data_key, obj)

    def _remove(self) -> None:
        # WHY: remove() calls _exists() first, so look the key up in the same repository, not in the one a dump would go to.
        self._repository().remove(self._data_key)

    def _last_modification_time(self) -> datetime:
        repository = self._repository()
        if not repository.has(self._data_key):
            raise ValueError(f'No object(s) which id is {self._data_key} are stored before.')
        return repository.get_last_modification_time(self._data_key)

    def is_persistent(self) -> bool:
        return False

    def _path(self) -> str:
        # TODO: this module name `_path` migit not be appropriate
        return self._data_key


# WHY keyword-only: the second positional argument used to be task_lock_params, which would otherwise be taken as unique_id silently.
def make_in_memory_target(target_key: str, *, unique_id: str | None = None) -> InMemoryTarget:
    data_key = _make_file_path(target_key, unique_id)
    return InMemoryTarget(data_key)
