from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from unittest.mock import patch

import pytest

from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.in_memory import InMemoryTarget, InMemoryTargetError, make_in_memory_target
from gokart.in_memory.repository import in_memory_scope


class TestInMemoryTarget:
    @pytest.fixture
    def target(self) -> InMemoryTarget:
        return make_in_memory_target(target_key='dummy_key')

    @pytest.fixture(autouse=True)
    def scope(self) -> Iterator[None]:
        with in_memory_scope():
            yield

    def test_dump_and_load_data(self, target: InMemoryTarget) -> None:
        dumped = 'dummy_data'
        target.dump(dumped)
        loaded = target.load()
        assert loaded == dumped

    def test_exist(self, target: InMemoryTarget) -> None:
        assert not target.exists()
        target.dump('dummy_data')
        assert target.exists()

    def test_last_modified_time(self, target: InMemoryTarget) -> None:
        input = 'dummy_data'
        with patch('gokart.in_memory.data.datetime') as mock_datetime:
            mock_datetime.now.side_effect = [datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 0, 1)]
            target.dump(input)
            time = target.last_modification_time()
            assert isinstance(time, datetime)

            another_input = 'another_data'
            target.dump(another_input)
            another_time = target.last_modification_time()
        assert time < another_time

        target.remove()
        with pytest.raises(ValueError):
            assert target.last_modification_time()

    def test_load_from_another_thread(self, target: InMemoryTarget) -> None:
        target.dump('dummy_data')
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(target.load).result() == 'dummy_data'

    def test_data_is_dropped_with_scope(self, target: InMemoryTarget) -> None:
        target.dump('dummy_data')
        with in_memory_scope():
            assert not target.exists()
        assert target.exists()

    def test_task_lock_is_not_supported(self) -> None:
        task_lock_params = TaskLockParams(
            redis_host='localhost',
            redis_port=6379,
            redis_timeout=None,
            redis_key='dummy',
            should_task_lock=True,
            raise_task_lock_exception_on_collision=False,
            lock_extend_seconds=0,
        )
        with pytest.raises(ValueError):
            make_in_memory_target(target_key='dummy_key', task_lock_params=task_lock_params)


class TestInMemoryTargetOutsideBuild:
    @pytest.fixture
    def target(self) -> InMemoryTarget:
        return make_in_memory_target(target_key='dummy_key')

    def test_exists_is_false(self, target: InMemoryTarget) -> None:
        assert not target.exists()

    def test_dump_raises(self, target: InMemoryTarget) -> None:
        with pytest.raises(InMemoryTargetError, match='gokart.build'):
            target.dump('dummy_data')

    def test_load_raises(self, target: InMemoryTarget) -> None:
        with pytest.raises(InMemoryTargetError, match='gokart.build'):
            target.load()
