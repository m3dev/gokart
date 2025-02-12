from typing import Any

from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_dump_with_lock, wrap_load_with_lock, wrap_remove_with_lock
from gokart.in_memory.repository import InMemoryCacheRepository


class CacheNotFoundError(OSError): ...


class CacheableMixin:
    def exists(self) -> bool:
        return self._cache_exists() or super().exists()

    def load(self) -> bool:
        def _load():
            cached = self._cache_exists()
            if cached:
                return self._cache_load()
            else:
                try:
                    loaded = super(CacheableMixin, self)._load()
                except FileNotFoundError as e:
                    raise CacheNotFoundError from e
                self._cache_dump(loaded)
                return loaded

        return wrap_load_with_lock(func=_load, task_lock_params=super()._get_task_lock_params())()

    def dump(self, obj: Any, lock_at_dump: bool = True, also_dump_to_file: bool = False):
        # TODO: how to sync cache and file
        def _dump(obj: Any):
            self._cache_dump(obj)
            if also_dump_to_file:
                super(CacheableMixin, self)._dump(obj)

        if lock_at_dump:
            wrap_dump_with_lock(func=_dump, task_lock_params=super()._get_task_lock_params(), exist_check=self.exists)(obj)
        else:
            _dump(obj)

    def remove(self, also_remove_file: bool = False):
        def _remove():
            if self._cache_exists():
                self._cache_remove()
            if super(CacheableMixin, self).exists() and also_remove_file:
                super(CacheableMixin, self)._remove()

        wrap_remove_with_lock(func=_remove, task_lock_params=super()._get_task_lock_params())()

    def last_modification_time(self):
        if self._cache_exists():
            return self._cache_last_modification_time()
        try:
            return super()._last_modification_time()
        except FileNotFoundError as e:
            raise CacheNotFoundError from e

    @property
    def data_key(self):
        return super().path()

    def _cache_exists(self):
        raise NotImplementedError

    def _cache_load(self):
        raise NotImplementedError

    def _cache_dump(self, obj: Any):
        raise NotImplementedError

    def _cache_remove(self):
        raise NotImplementedError

    def _cache_last_modification_time(self):
        raise NotImplementedError


class InMemoryCacheableMixin(CacheableMixin):
    _repository = InMemoryCacheRepository()

    def _cache_exists(self):
        return self._repository.has(self.data_key)

    def _cache_load(self):
        return self._repository.get_value(self.data_key)

    def _cache_dump(self, obj):
        return self._repository.set_value(self.data_key, obj)

    def _cache_remove(self):
        self._repository.remove(self.data_key)

    def _cache_last_modification_time(self):
        return self._repository.get_last_modification_time(self.data_key)
