import time

import pytest

from gokart.in_memory import InMemoryCacheRepository
from gokart.in_memory.repository import InstantScheduler

dummy_num = 100


class TestInMemoryCacheRepository:
    @pytest.fixture
    def repo(self):
        repo = InMemoryCacheRepository()
        repo.clear()
        return repo

    def test_set(self, repo: InMemoryCacheRepository):
        repo.set_value('dummy_key', dummy_num)
        assert repo.size == 1
        for key, value in repo.get_gen():
            assert (key, value) == ('dummy_key', dummy_num)

        repo.set_value('another_key', 'another_value')
        assert repo.size == 2

    def test_get(self, repo: InMemoryCacheRepository):
        repo.set_value('dummy_key', dummy_num)
        repo.set_value('another_key', 'another_value')

        """Raise Error when key doesn't exist."""
        with pytest.raises(KeyError):
            repo.get_value('not_exist_key')

        assert repo.get_value('dummy_key') == dummy_num
        assert repo.get_value('another_key') == 'another_value'

    def test_empty(self, repo: InMemoryCacheRepository):
        assert repo.empty()
        repo.set_value('dummmy_key', dummy_num)
        assert not repo.empty()

    def test_has(self, repo: InMemoryCacheRepository):
        assert not repo.has('dummy_key')
        repo.set_value('dummy_key', dummy_num)
        assert repo.has('dummy_key')
        assert not repo.has('not_exist_key')

    def test_remove(self, repo: InMemoryCacheRepository):
        repo.set_value('dummy_key', dummy_num)

        with pytest.raises(AssertionError):
            repo.remove('not_exist_key')

        repo.remove('dummy_key')
        assert not repo.has('dummy_key')

    def test_last_modification_time(self, repo: InMemoryCacheRepository):
        repo.set_value('dummy_key', dummy_num)
        date1 = repo.get_last_modification_time('dummy_key')
        time.sleep(0.1)
        repo.set_value('dummy_key', dummy_num)
        date2 = repo.get_last_modification_time('dummy_key')
        assert date1 < date2


class TestInstantScheduler:
    @pytest.fixture(autouse=True)
    def set_scheduler(self):
        scheduler = InstantScheduler()
        InMemoryCacheRepository.set_scheduler(scheduler)

    @pytest.fixture(autouse=True)
    def clear_cache(self):
        InMemoryCacheRepository.clear()

    @pytest.fixture
    def repo(self):
        repo = InMemoryCacheRepository()
        return repo

    def test_identity(self):
        scheduler1 = InstantScheduler()
        scheduler2 = InstantScheduler()
        assert id(scheduler1) == id(scheduler2)

    def test_scheduler_type(self, repo: InMemoryCacheRepository):
        assert isinstance(repo.scheduler, InstantScheduler)

    def test_volatility(self, repo: InMemoryCacheRepository):
        assert repo.empty()
        repo.set_value('dummy_key', 100)
        assert repo.has('dummy_key')
        repo.get_value('dummy_key')
        assert not repo.has('dummy_key')
