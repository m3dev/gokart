from gokart.in_memory import InMemeryCacheRepository as Repo
import pytest

dummy_num = 100

class TestInMemoryCacheRepository:
    @pytest.fixture
    def repo(self):
        repo = Repo()
        repo.clear()
        return repo

    def test_set(self, repo: Repo):
        repo.set("dummy_id", dummy_num)
        assert repo.size == 1
        for key, value in repo.get_gen():
            assert (key, value) == ("dummy_id", dummy_num)
        
        with pytest.raises(AssertionError):
            repo.set('dummy_id', "dummy_value")
        
        repo.set('another_id', 'another_value')
        assert repo.size == 2

    def test_get(self, repo: Repo):
        repo.set('dummy_id', dummy_num)
        repo.set('another_id', 'another_val')

        """Raise Error when key doesn't exist."""
        with pytest.raises(KeyError):
            repo.get('not_exist_id')
        
        assert repo.get('dummy_id') == dummy_num
        assert repo.get('another_id') == 'another_val'

    def test_empty(self, repo: Repo):
        assert repo.empty()
        repo.set("dummmy_id", dummy_num)
        assert not repo.empty()
    
    def test_has(self, repo: Repo):
        assert not repo.has('dummy_id')
        repo.set('dummy_id', dummy_num)
        assert repo.has('dummy_id')
    
    def test_remove_by_id(self, repo: Repo):
        repo.set('dummy_id', dummy_num)

        with pytest.raises(AssertionError):
            repo.remove_by_id('not_exist_id')

        assert repo.has('dummy_id')
        repo.remove_by_id('dummy_id')
        assert not repo.has('dummy_id')
