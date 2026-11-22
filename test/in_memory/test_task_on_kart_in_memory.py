import json
import random
from pathlib import Path

import luigi
import pytest
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]

import gokart
from gokart.file_processor import PickleFileProcessor
from gokart.in_memory import InMemoryTarget, InMemoryTargetError
from gokart.in_memory.repository import acquire_repository, in_memory_scope
from gokart.required_task_output import RequiredTaskOutput


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


class _LuigiTargetTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    path: luigi.Parameter[str] = luigi.Parameter()

    def output(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        # output() may return a plain luigi target at runtime despite its annotation.
        return luigi.LocalTarget(self.path)


class _ModificationTimeCheckTask(_LuigiTargetTask):
    """Outputs the same plain luigi target as its input, so the target is common to both and its timestamp is not read."""

    task_namespace = __name__

    def requires(self):
        return _LuigiTargetTask(path=self.path)


class _FileJoinTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    tasks: gokart.ListTaskInstanceParameter[gokart.TaskOnKart[str]] = gokart.ListTaskInstanceParameter()

    def output(self):
        return self.make_target('file_join.pkl')

    def run(self):
        self.dump('file join')

    def requires(self):
        return {'tasks': self.tasks}


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

    def test_plain_luigi_target_is_not_in_memory(self, tmp_path: Path) -> None:
        task = _LuigiTargetTask(path=str(tmp_path / 'luigi.txt'))
        assert not task._has_only_in_memory_outputs()


class TestModificationTimeCheck:
    def test_plain_luigi_target_common_to_input_and_output(self, tmp_path: Path) -> None:
        path = tmp_path / 'luigi.txt'
        path.write_text('plain')
        assert _ModificationTimeCheckTask(path=str(path), modification_time_check=True).complete()


class TestGCSWorkspace:
    def test_build_writes_nothing_to_gcs(self, gcs_bucket: str) -> None:
        workspace = f'gs://{gcs_bucket}/resources'
        task = _JoinTask(
            tasks=[_DumpTask(param='a', workspace_directory=workspace), _DumpTask(param='b', workspace_directory=workspace)],
            workspace_directory=workspace,
        )
        assert gokart.build(task, reset_register=False) == 'a,b'
        assert list(storage.Client().list_blobs(gcs_bucket)) == []

    def test_file_output_is_still_written_to_gcs(self, gcs_bucket: str) -> None:
        # Guards the test above: the emulator does receive what a file output dumps.
        gokart.build(_MixedOutputTask(workspace_directory=f'gs://{gcs_bucket}/resources'), reset_register=False, return_value=False)
        names = [blob.name for blob in storage.Client().list_blobs(gcs_bucket)]
        assert any(name.startswith('resources/file_') for name in names)
        assert not any(name.startswith('resources/memory_') for name in names)

    def test_in_memory_input_is_not_labeled_as_gcs_object(self, gcs_bucket: str) -> None:
        # The key of an in-memory output starts with gs:// like a file path, but no object exists there.
        workspace = f'gs://{gcs_bucket}/resources'
        file_task = _MixedOutputTask(workspace_directory=workspace)
        task = _FileJoinTask(tasks=[_DumpTask(param='a', workspace_directory=workspace), file_task], workspace_directory=workspace)
        gokart.build(task, reset_register=False, return_value=False)
        [blob] = [blob for blob in storage.Client().list_blobs(gcs_bucket) if blob.name.startswith('resources/file_join_')]
        file_output_path = file_task.output()['file'].path()
        assert json.loads(blob.metadata['__required_task_outputs']) == {
            'tasks': [{'file': {'__gokart_task_name': f'{__name__}._MixedOutputTask', '__gokart_output_path': file_output_path}}]
        }


class TestDeleteLocalUnnecessaryOutputs:
    def test_file_at_in_memory_key_is_deleted(self, tmp_path: Path) -> None:
        # e.g. a file left by the task before it switched to persist=False; the in-memory output does not use it.
        task = _DumpTask(param='a', workspace_directory=str(tmp_path))
        stale_file = Path(task.output().path())
        stale_file.write_bytes(b'stale')
        gokart.delete_local_unnecessary_outputs(task)
        assert not stale_file.exists()


class TestRequiredTaskOutputs:
    def test_in_memory_outputs_are_dropped(self, tmp_path: Path) -> None:
        file_task = _MixedOutputTask(workspace_directory=str(tmp_path))
        task = _FileJoinTask(tasks=[_DumpTask(param='a'), file_task], workspace_directory=str(tmp_path))
        expected = RequiredTaskOutput(task_name=file_task.get_task_family(), output_path=file_task.output()['file'].path())
        assert task._get_required_task_outputs() == {'tasks': ({'file': expected},)}

    def test_only_in_memory_outputs_give_none(self, tmp_path: Path) -> None:
        task = _FileJoinTask(tasks=[_DumpTask(param='a')], workspace_directory=str(tmp_path))
        assert task._get_required_task_outputs() is None

    def test_empty_requirements_without_in_memory_outputs_are_kept(self, tmp_path: Path) -> None:
        # The label of a task without in-memory inputs must stay as it was before in-memory outputs were dropped.
        task = _FileJoinTask(tasks=[], workspace_directory=str(tmp_path))
        assert task._get_required_task_outputs() == {'tasks': ()}
