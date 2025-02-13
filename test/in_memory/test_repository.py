import time

import pytest

from gokart.in_memory import InMemoryCacheRepository as Repo

dummy_num = 100


class TestInMemoryCacheRepository:
    @pytest.fixture
    def repo(self) -> Repo:
        repo = Repo()
        repo.clear()
        return repo

    def test_set(self, repo: Repo):
        repo.set_value('dummy_key', dummy_num)
        assert repo.size == 1
        for key, value in repo.get_gen():
            assert (key, value) == ('dummy_key', dummy_num)

        repo.set_value('another_key', 'another_value')
        assert repo.size == 2

    def test_get(self, repo: Repo):
        repo.set_value('dummy_key', dummy_num)
        repo.set_value('another_key', 'another_value')

        """Raise Error when key doesn't exist."""
        with pytest.raises(KeyError):
            repo.get_value('not_exist_key')

        assert repo.get_value('dummy_key') == dummy_num
        assert repo.get_value('another_key') == 'another_value'

    def test_empty(self, repo: Repo):
        assert repo.empty()
        repo.set_value('dummmy_key', dummy_num)
        assert not repo.empty()

    def test_has(self, repo: Repo):
        assert not repo.has('dummy_key')
        repo.set_value('dummy_key', dummy_num)
        assert repo.has('dummy_key')
        assert not repo.has('not_exist_key')

    def test_remove(self, repo: Repo):
        repo.set_value('dummy_key', dummy_num)

        with pytest.raises(AssertionError):
            repo.remove('not_exist_key')

        repo.remove('dummy_key')
        assert not repo.has('dummy_key')

    def test_last_modification_time(self, repo: Repo):
        repo.set_value('dummy_key', dummy_num)
        date1 = repo.get_last_modification_time('dummy_key')
        time.sleep(0.1)
        repo.set_value('dummy_key', dummy_num)
        date2 = repo.get_last_modification_time('dummy_key')
        assert date1 < date2
