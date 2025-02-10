from typing import Any, Iterator

from .data import InMemoryData


class BaseRepository: ...


class InMemoryCacheRepository(BaseRepository):
    _cache: dict[str, InMemoryData] = {}

    def __init__(self):
        pass

    def get_value(self, key: str) -> Any:
        return self._get_data(key).value

    def get_last_modification_time(self, key: str):
        return self._get_data(key).last_modified_time

    def _get_data(self, id: str) -> InMemoryData:
        return self._cache[id]

    def set_value(self, id: str, obj: Any) -> None:
        data = InMemoryData.create_data(obj)
        self._cache[id] = data

    def has(self, id: str) -> bool:
        return id in self._cache

    def remove(self, id: str) -> None:
        assert self.has(id)
        del self._cache[id]

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
