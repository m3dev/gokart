from datetime import datetime
from time import sleep

import pytest

from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.in_memory import InMemoryCacheRepository, InMemoryTarget, make_in_memory_target


class TestInMemoryTarget:
    @pytest.fixture
    def task_lock_params(self) -> TaskLockParams:
        return TaskLockParams(
            redis_host=None,
            redis_port=None,
            redis_timeout=None,
            redis_key='dummy',
            should_task_lock=False,
            raise_task_lock_exception_on_collision=False,
            lock_extend_seconds=0,
        )

    @pytest.fixture
    def target(self, task_lock_params: TaskLockParams) -> InMemoryTarget:
        return make_in_memory_target(target_key='dummy_key', task_lock_params=task_lock_params)

    @pytest.fixture(autouse=True)
    def clear_repo(self) -> None:
        InMemoryCacheRepository().clear()

    def test_dump_and_load_data(self, target: InMemoryTarget):
        dumped = 'dummy_data'
        target.dump(dumped)
        loaded = target.load()
        assert loaded == dumped

    def test_exist(self, target: InMemoryTarget):
        assert not target.exists()
        target.dump('dummy_data')
        assert target.exists()

    def test_last_modified_time(self, target: InMemoryTarget):
        input = 'dummy_data'
        target.dump(input)
        time = target.last_modification_time()
        assert isinstance(time, datetime)

        sleep(0.1)
        another_input = 'another_data'
        target.dump(another_input)
        another_time = target.last_modification_time()
        assert time < another_time

        target.remove()
        with pytest.raises(ValueError):
            assert target.last_modification_time()
