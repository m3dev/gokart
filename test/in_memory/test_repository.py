from datetime import datetime
from unittest.mock import patch

import pytest

from gokart.in_memory import InMemoryCacheRepository as Repo
from gokart.in_memory.repository import get_current_repository, in_memory_scope

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

    def test_scope_provides_empty_repository_and_drops_it_on_exit(self) -> None:
        with in_memory_scope() as repo:
            assert get_current_repository() is repo
            assert repo.empty()
            repo.set_value('dummy_key', dummy_num)
        assert get_current_repository() is None

    def test_nested_scope_has_its_own_repository(self) -> None:
        with in_memory_scope() as outer:
            outer.set_value('dummy_key', dummy_num)
            with in_memory_scope() as inner:
                assert not inner.has('dummy_key')
            assert get_current_repository() is outer
