from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.in_memory import make_inmemory_target, InMemoryTarget, InMemeryCacheRepository
import pytest

class TestInMemoryTarget:
    @pytest.fixture
    def task_lock_params(self):
        return TaskLockParams(
            redis_host=None,
            redis_port=None,
            redis_timeout=None,
            redis_key='dummy',
            should_task_lock=False,
            raise_task_lock_exception_on_collision=False,
            lock_extend_seconds=0
        )
    @pytest.fixture
    def target(self, task_lock_params: TaskLockParams):
        return make_inmemory_target(target_key='dummy_task_id', task_lock_params=task_lock_params)
    
    @pytest.fixture(autouse=True)
    def clear_repo(self):
        InMemeryCacheRepository().clear()

    def test_dump_and_load_data(self, target: InMemoryTarget):
        dumped = 'dummy_data'
        target.dump(dumped)
        loaded = target.load()
        assert loaded == dumped

        with pytest.raises(AssertionError):
            target.dump('another_data')