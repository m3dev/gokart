import abc
from typing import Any

class BaseRepository(abc.ABC):
    ...

class InMemeryCacheRepository(BaseRepository):
    _cache: dict[str, Any] = {}
    def __init__(self):
        pass

    def get(self, id: str):
        return self._cache[id]
    
    def set(self, id: str, obj: Any):
        assert not self.has(id)
        self._cache[id] = obj
    
    def has(self, id: str):
        return id in self._cache
    
    def remove_by_id(self, id: str):
        assert self.has(id)
        del self._cache[id]

    def empty(self):
        return not self._cache
    
    def clear(self):
        self._cache.clear()

    def get_gen(self):
        for key, value in self._cache.items():
            yield key, value

    @property
    def size(self):
        return len(self._cache)