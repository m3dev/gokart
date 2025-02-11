from typing import Optional, Type, Union

import luigi
import pytest

import gokart
from gokart.in_memory import InMemoryCacheRepository, InMemoryTarget
from gokart.target import SingleFileTarget


class DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    param: str = luigi.Parameter()

    def run(self):
        self.dump(self.param)


class DummyTaskWithDependencies(gokart.TaskOnKart):
    task_namespace = __name__
    task: list[gokart.TaskOnKart[str]] = gokart.ListTaskInstanceParameter()

    def run(self):
        result = ','.join(self.load())
        self.dump(result)


class DumpIntTask(gokart.TaskOnKart[int]):
    task_namespace = __name__
    value: int = luigi.IntParameter()

    def run(self):
        self.dump(self.value)


class AddTask(gokart.TaskOnKart[Union[int, float]]):
    a: gokart.TaskOnKart[int] = gokart.TaskInstanceParameter()
    b: gokart.TaskOnKart[int] = gokart.TaskInstanceParameter()

    def requires(self):
        return dict(a=self.a, b=self.b)

    def run(self):
        a = self.load(self.a)
        b = self.load(self.b)
        self.dump(a + b)


class TestTaskOnKartWithCache:
    @pytest.fixture(autouse=True)
    def clear_repository(slef):
        InMemoryCacheRepository().clear()

    @pytest.mark.parametrize('data_key', ['sample_key', None])
    @pytest.mark.parametrize('use_unique_id', [True, False])
    def test_key_identity(self, data_key: Optional[str], use_unique_id: bool):
        task = DummyTask(param='param')
        ext = '.pkl'
        relative_file_path = data_key + ext if data_key else None
        target = task.make_target(relative_file_path=relative_file_path, use_unique_id=use_unique_id)
        cached_target = task.make_cache_target(data_key=data_key, use_unique_id=use_unique_id)

        target_path = target.path().removeprefix(task.workspace_directory).removesuffix(ext).strip('/')
        assert cached_target.path() == target_path

    def test_make_cached_target(self):
        task = DummyTask(param='param')
        target = task.make_cache_target()
        assert isinstance(target, InMemoryTarget)

    @pytest.mark.parametrize(['cache_in_memory_by_default', 'target_type'], [[True, InMemoryTarget], [False, SingleFileTarget]])
    def test_make_default_target(self, cache_in_memory_by_default: bool, target_type: Type[gokart.TaskOnKart]):
        task = DummyTask(param='param', cache_in_memory_by_default=cache_in_memory_by_default)
        target = task.output()
        assert isinstance(target, target_type)

    def test_complete_with_cache_in_memory_flag(self, tmpdir):
        task = DummyTask(param='param', cache_in_memory_by_default=True, workspace_directory=tmpdir)
        assert not task.complete()
        file_target = task.make_target()
        file_target.dump('data')
        assert not task.complete()
        cache_target = task.make_cache_target()
        cache_target.dump('data')
        assert task.complete()

    def test_complete_without_cache_in_memory_flag(self, tmpdir):
        task = DummyTask(param='param', workspace_directory=tmpdir)
        assert not task.complete()
        cache_target = task.make_cache_target()
        cache_target.dump('data')
        assert not task.complete()
        file_target = task.make_target()
        file_target.dump('data')
        assert task.complete()

    def test_dump_with_cache_in_memory_flag(self, tmpdir):
        task = DummyTask(param='param', cache_in_memory_by_default=True, workspace_directory=tmpdir)
        file_target = task.make_target()
        cache_target = task.make_cache_target()
        task.dump('data')
        assert not file_target.exists()
        assert cache_target.exists()

    def test_dump_without_cache_in_memory_flag(self, tmpdir):
        task = DummyTask(param='param', workspace_directory=tmpdir)
        file_target = task.make_target()
        cache_target = task.make_cache_target()
        task.dump('data')
        assert file_target.exists()
        assert not cache_target.exists()

    def test_gokart_build(self):
        task = AddTask(
            a=DumpIntTask(value=2, cache_in_memory_by_default=True), b=DumpIntTask(value=3, cache_in_memory_by_default=True), cache_in_memory_by_default=True
        )
        output = gokart.build(task, reset_register=False)
        assert output == 5
