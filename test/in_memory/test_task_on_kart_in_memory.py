import random
from pathlib import Path

import luigi
import pytest

import gokart
from gokart.file_processor import PickleFileProcessor
from gokart.in_memory import InMemoryTarget, InMemoryTargetError
from gokart.in_memory.repository import acquire_repository, in_memory_scope


class _DumpTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    param: luigi.Parameter[str] = luigi.Parameter()

    def output(self):
        return self.make_target('dump.pkl', persist=False)

    def run(self):
        self.dump(self.param)


class _JoinTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    tasks: gokart.ListTaskInstanceParameter[gokart.TaskOnKart[str]] = gokart.ListTaskInstanceParameter()

    def output(self):
        return self.make_target('join.pkl', persist=False)

    def run(self):
        self.dump(','.join(self.load('tasks')))

    def requires(self):
        return {'tasks': self.tasks}


class _UnusableOutputTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def output(self):
        raise RuntimeError('output() must not be called')


class _RandomTask(gokart.TaskOnKart[float]):
    task_namespace = __name__

    def output(self):
        return self.make_target('random.pkl', persist=False)

    def run(self):
        self.dump(random.random())


class _MixedOutputTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def output(self):
        return {'memory': self.make_target('memory.pkl', persist=False), 'file': self.make_target('file.pkl')}

    def run(self):
        self.dump('memory', 'memory')
        self.dump('file', 'file')


class TestMakeInMemoryTarget:
    @pytest.mark.parametrize('use_unique_id', [True, False])
    def test_key_is_same_as_file_path(self, use_unique_id: bool) -> None:
        task = _DumpTask(param='a')
        target = task.make_target('dir/data.pkl', use_unique_id=use_unique_id, persist=False)
        assert isinstance(target, InMemoryTarget)
        assert target.path() == task.make_target('dir/data.pkl', use_unique_id=use_unique_id).path()

    def test_default_key_is_same_as_file_path(self) -> None:
        task = _DumpTask(param='a')
        assert task.make_target(persist=False).path() == task.make_target().path()

    def test_key_differs_by_parameters(self) -> None:
        assert _DumpTask(param='a').output().path() != _DumpTask(param='b').output().path()

    def test_persist_is_true_by_default(self) -> None:
        assert not isinstance(_DumpTask(param='a').make_target('data.pkl'), InMemoryTarget)

    def test_processor_is_not_supported(self) -> None:
        with pytest.raises(ValueError, match='persist=False'):
            _DumpTask(param='a').make_target('data.pkl', processor=PickleFileProcessor(), persist=False)

    def test_should_lock_run_is_not_supported(self) -> None:
        task = _DumpTask(param='a', should_lock_run=True, redis_host='localhost', redis_port=6379)
        with pytest.raises(InMemoryTargetError, match='should_lock_run'):
            gokart.build(task, reset_register=False)

    def test_dump_and_load_lock_is_skipped(self) -> None:
        task = _DumpTask(param='a', redis_host='localhost', redis_port=6379)
        with in_memory_scope():
            acquire_repository()
            task.dump('a')
            assert task.load(task.output()) == 'a'

    def test_gcs_labels_are_not_computed_on_gcs_workspace(self) -> None:
        # Computing the labels would call output() of the required task, which raises.
        task = _JoinTask(tasks=[_UnusableOutputTask()], workspace_directory='gs://bucket/resources')
        with in_memory_scope():
            acquire_repository()
            task.dump('a')
            assert task.load(task.output()) == 'a'


class TestBuildWithMakeInMemoryTarget:
    def test_build_returns_value_without_output_files(self, tmp_path: Path) -> None:
        workspace = str(tmp_path / 'resources')
        task = _JoinTask(
            tasks=[_DumpTask(param='a', workspace_directory=workspace), _DumpTask(param='b', workspace_directory=workspace)],
            workspace_directory=workspace,
        )
        assert gokart.build(task, reset_register=False) == 'a,b'
        assert not list(Path(workspace).rglob('dump*.pkl'))
        assert not list(Path(workspace).rglob('join*.pkl'))


class TestSupplementaryLogFiles:
    def test_not_dumped_when_all_outputs_are_in_memory(self, tmp_path: Path) -> None:
        workspace = str(tmp_path / 'resources')
        gokart.build(_DumpTask(param='a', workspace_directory=workspace), reset_register=False)
        assert not list(Path(workspace).rglob('*'))

    def test_random_seed_is_still_fixed(self, tmp_path: Path) -> None:
        task = _RandomTask(workspace_directory=str(tmp_path / 'resources'))
        random.seed(task._get_random_seed())
        expected = random.random()
        random.seed()
        assert gokart.build(task, reset_register=False) == expected

    def test_dumped_when_some_outputs_are_files(self, tmp_path: Path) -> None:
        workspace = str(tmp_path / 'resources')
        gokart.build(_MixedOutputTask(workspace_directory=workspace), reset_register=False, return_value=False)
        assert list(Path(workspace).rglob('log/task_log/*.pkl'))
