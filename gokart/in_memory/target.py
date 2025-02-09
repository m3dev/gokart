from gokart.target import TargetOnKart, TaskLockParams
from gokart.in_memory.repository import InMemeryCacheRepository
from datetime import datetime

_repository = InMemeryCacheRepository()

# TODO: unnecessary params in task_lock_param expecially regarding redies
class InMemoryTarget(TargetOnKart):
    def __init__(
            self,
            id: str,
            task_lock_param: TaskLockParams
        ):
        self._id = id
        self._task_lock_params = task_lock_param
        self._last_modification_time_value: None | datetime = None
    
    def _exists(self):
        # import pdb;pdb.set_trace()
        return _repository.has(self._id)
    
    def _get_task_lock_params(self):
        return self._task_lock_params
    
    def _load(self):
        # import pdb
        # pdb.set_trace()
        return _repository.get(self._id)
    
    def _dump(self, obj):
        return _repository.set(self._id, obj)
    
    def _remove(self) -> None:
        _repository.remove_by_id(self._id)
    
    def _last_modification_time(self) -> datetime:
        if self._last_modification_time_value is None:
            raise ValueError(f"No object(s) which id is {self._id} are stored before.")
        self._last_modification_time_value
    
    def _path(self):
        # TODO: this module name `_path` migit not be appropriate
        return self._id

    @property
    def id(self):
        return self._id

def make_inmemory_target(target_key: str, task_lock_params: TaskLockParams | None = None):
    return InMemoryTarget(target_key, task_lock_params)
