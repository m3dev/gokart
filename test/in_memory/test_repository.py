from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from unittest.mock import patch

import pytest

from gokart.in_memory import InMemoryCacheRepository as Repo
from gokart.in_memory.repository import InMemoryTargetError, acquire_repository, get_current_repository, in_memory_scope

dummy_num = 100


class TestInMemoryCacheRepository:
    @pytest.fixture
    def repo(self) -> Repo:
        return Repo()

    def test_set(self, repo: Repo) -> None:
        repo.set_value('dummy_key', dummy_num)
        assert repo.size == 1
        for key, value in repo.get_gen():
            assert (key, value) == ('dummy_key', dummy_num)

        repo.set_value('another_key', 'another_value')
        assert repo.size == 2

    def test_get(self, repo: Repo) -> None:
        repo.set_value('dummy_key', dummy_num)
        repo.set_value('another_key', 'another_value')

        """Raise Error when key doesn't exist."""
        with pytest.raises(KeyError):
            repo.get_value('not_exist_key')

        assert repo.get_value('dummy_key') == dummy_num
        assert repo.get_value('another_key') == 'another_value'

    def test_empty(self, repo: Repo) -> None:
        assert repo.empty()
        repo.set_value('dummmy_key', dummy_num)
        assert not repo.empty()

    def test_has(self, repo: Repo) -> None:
        assert not repo.has('dummy_key')
        repo.set_value('dummy_key', dummy_num)
        assert repo.has('dummy_key')
        assert not repo.has('not_exist_key')

    def test_remove(self, repo: Repo) -> None:
        repo.set_value('dummy_key', dummy_num)

        with pytest.raises(AssertionError):
            repo.remove('not_exist_key')

        repo.remove('dummy_key')
        assert not repo.has('dummy_key')

    def test_last_modification_time(self, repo: Repo) -> None:
        with patch('gokart.in_memory.data.datetime') as mock_datetime:
            mock_datetime.now.side_effect = [datetime(2024, 1, 1, 0, 0, 0), datetime(2024, 1, 1, 0, 0, 1)]
            repo.set_value('dummy_key', dummy_num)
            date1 = repo.get_last_modification_time('dummy_key')
            repo.set_value('dummy_key', dummy_num)
            date2 = repo.get_last_modification_time('dummy_key')
        assert date1 < date2

    def test_repositories_do_not_share_data(self) -> None:
        Repo().set_value('dummy_key', dummy_num)
        assert not Repo().has('dummy_key')


class TestInMemoryScope:
    def test_no_repository_outside_scope(self) -> None:
        assert get_current_repository() is None
        with pytest.raises(InMemoryTargetError, match='gokart.build'):
            acquire_repository()

    def test_repository_is_created_on_first_acquire_and_dropped_on_exit(self) -> None:
        with in_memory_scope():
            assert get_current_repository() is None
            repo = acquire_repository()
            assert acquire_repository() is repo
            assert get_current_repository() is repo
            assert repo.empty()
        assert get_current_repository() is None

    def test_nested_scope_has_its_own_repository(self) -> None:
        with in_memory_scope():
            outer = acquire_repository()
            outer.set_value('dummy_key', dummy_num)
            with in_memory_scope():
                # Until the inner build acquires its own repository, it must not see the outer build's outputs.
                assert get_current_repository() is None
                assert not acquire_repository().has('dummy_key')
            assert get_current_repository() is outer

    def test_thread_outside_any_build_sees_repository(self) -> None:
        with in_memory_scope():
            repo = acquire_repository()
            with ThreadPoolExecutor(max_workers=1) as executor:
                assert executor.submit(get_current_repository).result() is repo

    def test_scope_in_another_thread_does_not_replace_repository(self) -> None:
        def build_without_in_memory_target() -> None:
            with in_memory_scope():
                pass

        with in_memory_scope():
            repo = acquire_repository()
            _run_in_thread(build_without_in_memory_target)
            assert get_current_repository() is repo

    def test_acquire_in_another_thread_while_repository_is_in_use_raises(self) -> None:
        def build_with_in_memory_target() -> None:
            with in_memory_scope():
                acquire_repository()

        with in_memory_scope():
            repo = acquire_repository()
            with pytest.raises(InMemoryTargetError, match='Another thread'):
                _run_in_thread(build_with_in_memory_target)
            assert get_current_repository() is repo

    def test_repository_can_be_acquired_in_another_thread_after_release(self) -> None:
        def build_with_in_memory_target() -> None:
            with in_memory_scope():
                acquire_repository()

        with in_memory_scope():
            acquire_repository()
        _run_in_thread(build_with_in_memory_target)


def _run_in_thread(func: Callable[[], None]) -> None:
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(func).result()
