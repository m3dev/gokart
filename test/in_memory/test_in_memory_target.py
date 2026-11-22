from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest

from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.in_memory import InMemoryTarget, InMemoryTargetError, make_in_memory_target
from gokart.in_memory.repository import acquire_repository, in_memory_scope


class TestInMemoryTarget:
    @pytest.fixture
    def target(self) -> InMemoryTarget:
        return make_in_memory_target(target_key='dummy_key')

    @pytest.fixture(autouse=True)
    def scope(self) -> Iterator[None]:
        with in_memory_scope():
            acquire_repository()
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

    def test_load_returns_dumped_object_itself(self, target: InMemoryTarget) -> None:
        dumped = {'a': [1]}
        target.dump(dumped)
        assert target.load() is dumped

    def test_column_added_to_loaded_data_frame_does_not_leak(self, target: InMemoryTarget) -> None:
        target.dump(pd.DataFrame({'a': [1, 2]}))
        loaded = target.load()
        loaded['b'] = [3, 4]
        loaded.loc[0, 'a'] = 100
        pd.testing.assert_frame_equal(target.load(), pd.DataFrame({'a': [1, 2]}))

    def test_data_frame_changed_after_dump_does_not_leak(self, target: InMemoryTarget) -> None:
        df = pd.DataFrame({'a': [1, 2]})
        target.dump(df)
        df['b'] = [3, 4]
        df.loc[0, 'a'] = 100
        pd.testing.assert_frame_equal(target.load(), pd.DataFrame({'a': [1, 2]}))

    def test_load_from_another_thread(self, target: InMemoryTarget) -> None:
        target.dump('dummy_data')
        with ThreadPoolExecutor(max_workers=1) as executor:
            assert executor.submit(target.load).result() == 'dummy_data'

    def test_data_is_dropped_with_scope(self, target: InMemoryTarget) -> None:
        target.dump('dummy_data')
        with in_memory_scope():
            acquire_repository()
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


class TestInMemoryTargetInThreadDuringNestedBuild:
    def test_remove_from_repository_that_has_key(self) -> None:
        # The nested build also outputs the key but has not dumped it yet, so exists() finds it in the outer build only.
        target = make_in_memory_target(target_key='shared_key')
        with in_memory_scope():
            outer = acquire_repository()
            outer.register('shared_key')
            outer.set_value('shared_key', 'outer')
            with in_memory_scope():
                acquire_repository().register('shared_key')
                with ThreadPoolExecutor(max_workers=1) as executor:
                    executor.submit(target.remove).result()
            assert not outer.has('shared_key')
