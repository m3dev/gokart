import pickle
from time import sleep

import luigi
import pytest

from gokart.in_memory import InMemoryCacheRepository
from gokart.in_memory.cacheable import CacheNotFoundError
from gokart.target import CacheableModelTarget, CacheableSingleFileTarget
from gokart.task import TaskOnKart


class DummyTask(TaskOnKart):
    namespace = __name__
    param = luigi.IntParameter()

    def run(self):
        self.dump(self.param)


class TestCacheableSingleFileTarget:
    @pytest.fixture
    def task(self, tmpdir):
        task = DummyTask(param=100, workspace_directory=tmpdir)
        return task

    @pytest.fixture(autouse=True)
    def clear_repository(self):
        InMemoryCacheRepository.clear()

    def test_exists_when_cache_exists(self, task: TaskOnKart):
        cacheable_target = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(cacheable_target.path(), use_unique_id=False)
        assert not cacheable_target.exists()
        cache_target.dump('data')
        assert cacheable_target.exists()

    def test_exists_when_file_exists(self, task: TaskOnKart):
        cacheable_target = task.make_target('sample.pkl', cacheable=True)
        target = task.make_target('sample.pkl')
        assert not cacheable_target.exists()
        target.dump('data')
        assert cacheable_target.exists()

    def test_load_without_cache_or_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        with pytest.raises(FileNotFoundError):
            target.load()
        cacheable_target = task.make_target('sample.pkl', cacheable=True)
        with pytest.raises(CacheNotFoundError):
            cacheable_target.load()

    def test_load_with_cache(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        with pytest.raises(CacheNotFoundError):
            cacheable_target.load()
        cache_target.dump('data')
        with pytest.raises(FileNotFoundError):
            target.load()
        assert cacheable_target.load() == 'data'
        assert cache_target.load() == 'data'

    def test_load_with_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        with pytest.raises(CacheNotFoundError):
            cacheable_target.load()
        target.dump('data')
        assert target.load() == 'data'
        with pytest.raises(KeyError):
            cache_target.load()
        assert cacheable_target.load() == 'data'
        assert cache_target.load() == 'data'

    def test_load_with_cache_and_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        with pytest.raises(CacheNotFoundError):
            cacheable_target.load()
        target.dump('data_in_file')
        cache_target.dump('data_in_memory')
        assert cacheable_target.load() == 'data_in_memory'

    def test_dump(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        cacheable_target.dump('data')
        assert not target.exists()
        assert cache_target.exists()
        assert cacheable_target.exists()

    def test_dump_with_dump_to_file_flag(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        cacheable_target.dump('data', also_dump_to_file=True)
        assert target.exists()
        assert cache_target.exists()
        assert cacheable_target.exists()

    def test_remove_without_cache_or_file(self, task: TaskOnKart):
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cacheable_target.remove()
        cacheable_target.remove(also_remove_file=True)
        assert True

    def test_remove_with_cache(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        cache_target.dump('data')
        assert cache_target.exists()
        cacheable_target.remove()
        assert not cache_target.exists()

    def test_remove_with_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        target.dump('data')
        assert target.exists()
        cacheable_target.remove()
        assert target.exists()
        cacheable_target.remove(also_remove_file=True)
        assert not target.exists()

    def test_remove_with_cache_and_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        target.dump('file_data')
        cache_target.dump('inmemory_data')
        cacheable_target.remove()
        assert target.exists()
        assert not cache_target.exists()

        target.dump('file_data')
        cache_target.dump('inmemory_data')
        cacheable_target.remove(also_remove_file=True)
        assert not target.exists()
        assert not cache_target.exists()

    def test_last_modification_time_without_cache_and_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        with pytest.raises(FileNotFoundError):
            target.last_modification_time()
        with pytest.raises(ValueError):
            cache_target.last_modification_time()
        with pytest.raises(CacheNotFoundError):
            cacheable_target.last_modification_time()

    def test_last_modification_time_with_cache(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        cache_target.dump('data')
        assert cacheable_target.last_modification_time() == cache_target.last_modification_time()

    def test_last_modification_time_with_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        target.dump('data')
        assert cacheable_target.last_modification_time() == target.last_modification_time()

    def test_last_modification_time_with_cache_and_file(self, task: TaskOnKart):
        target = task.make_target('sample.pkl')
        cacheable_target: CacheableSingleFileTarget = task.make_target('sample.pkl', cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        target.dump('file_data')
        sleep(0.1)
        cache_target.dump('inmemory_data')
        assert cacheable_target.last_modification_time() == cache_target.last_modification_time()


class DummyModule:
    def func(self):
        return 'hello world'


class DummyModuleA:
    def func_a(self):
        return 'hello world'


class DummyModuleB:
    def func_b(self):
        return 'hello world'


def _save_func(obj, path):
    with open(path, 'wb') as f:
        pickle.dump(obj, f)


def _load_func(path):
    with open(path, 'rb') as f:
        return pickle.load(f)


class TestCacheableModelTarget:
    @pytest.fixture
    def task(self, tmpdir):
        task = DummyTask(param=100, workspace_directory=tmpdir)
        return task

    @pytest.fixture(autouse=True)
    def clear_repository(self):
        InMemoryCacheRepository.clear()

    def test_exists_without_cache_or_file(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        assert not target.exists()
        assert not cache_target.exists()
        assert not cacheable_target.exists()

    def test_exists_with_file(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        assert not cacheable_target.exists()
        module = DummyModule()
        target.dump(module)
        assert cacheable_target.exists()

    def test_exists_with_cache(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        assert not cacheable_target.exists()
        module = DummyModule()
        cache_target.dump(module)
        assert not target.exists()
        assert cacheable_target.exists()

    def test_load_without_cache_or_file(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        with pytest.raises(FileNotFoundError):
            target.load()
        with pytest.raises(KeyError):
            cache_target.load()
        with pytest.raises(CacheNotFoundError):
            cacheable_target.load()

    def test_load_with_cache(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        module = DummyModule()
        cache_target.dump(module)
        with pytest.raises(FileNotFoundError):
            target.load()
        assert isinstance(cache_target.load(), DummyModule)
        assert isinstance(cacheable_target.load(), DummyModule)

    def test_load_with_file(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        module = DummyModule()
        target.dump(module)
        assert isinstance(target.load(), DummyModule)
        assert not cache_target.exists()
        assert isinstance(cacheable_target.load(), DummyModule)
        assert cache_target.exists()

    def test_load_with_cache_and_file(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True)
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        inmemory_module_cls, file_module_cls = DummyModule, DummyModuleA
        inmemory_module, file_module = inmemory_module_cls(), file_module_cls()
        target.dump(file_module)
        cache_target.dump(inmemory_module)
        assert isinstance(target.load(), file_module_cls)
        assert isinstance(cache_target.load(), inmemory_module_cls)
        assert isinstance(cacheable_target.load(), inmemory_module_cls)

    def test_dump(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target: CacheableModelTarget = task.make_model_target(
            relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True
        )
        module = DummyModule()
        cacheable_target.dump(module)
        assert not target.exists()
        assert cacheable_target.exists()

    def test_dump_with_dump_to_file_flag(self, task: TaskOnKart):
        target = task.make_model_target(relative_file_path='model.zip', save_function=_save_func, load_function=_load_func)
        cacheable_target: CacheableModelTarget = task.make_model_target(
            relative_file_path='model.zip', save_function=_save_func, load_function=_load_func, cacheable=True
        )
        cache_target = task.make_cache_target(target.path(), use_unique_id=False)
        module = DummyModule()
        cacheable_target.dump(module, also_dump_to_file=True)
        assert target.exists()
        assert cache_target.exists()
        assert cacheable_target.exists()
