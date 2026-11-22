"""TaskOnKart.output() may return a plain luigi target despite its annotation, whose path is an attribute, not a method."""

import os
from pathlib import Path

import luigi

import gokart
from gokart.required_task_output import RequiredTaskOutput
from gokart.tree.task_info_formatter import make_task_info_tree


class _LocalTargetTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    path: luigi.Parameter[str] = luigi.Parameter()

    def output(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        return luigi.LocalTarget(self.path)

    def run(self):
        with self.output().open('w') as f:
            f.write('plain')


class _DownstreamTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    path: luigi.Parameter[str] = luigi.Parameter()

    def requires(self):
        return _LocalTargetTask(path=self.path)

    def output(self):
        return self.make_target('downstream.pkl')

    def run(self):
        self.dump('downstream')


def _set_mtime(path: str, timestamp: float) -> None:
    os.utime(path, (timestamp, timestamp))


class TestModificationTimeCheck:
    def test_compares_with_local_target_input(self, tmp_path: Path) -> None:
        task = _DownstreamTask(path=str(tmp_path / 'plain.txt'), workspace_directory=str(tmp_path / 'resources'))
        gokart.build(task, reset_register=False, return_value=False)
        input_path = str(tmp_path / 'plain.txt')
        output_path = task.output().path()

        _set_mtime(input_path, 1_000)
        _set_mtime(output_path, 2_000)
        assert _DownstreamTask(path=input_path, workspace_directory=str(tmp_path / 'resources'), modification_time_check=True).complete()

        _set_mtime(input_path, 3_000)
        assert not _DownstreamTask(path=input_path, workspace_directory=str(tmp_path / 'resources'), modification_time_check=True).complete()


class TestDeleteLocalUnnecessaryOutputs:
    def test_keeps_local_target_output(self, tmp_path: Path) -> None:
        workspace = tmp_path / 'resources'
        task = _DownstreamTask(path=str(workspace / 'plain.txt'), workspace_directory=str(workspace))
        gokart.build(task, reset_register=False, return_value=False)
        stale_file = workspace / 'stale.txt'
        stale_file.write_text('stale')

        gokart.delete_local_unnecessary_outputs(task)

        assert (workspace / 'plain.txt').exists()
        assert Path(task.output().path()).exists()
        assert not stale_file.exists()


class TestRequiredTaskOutputs:
    def test_local_target_output_path(self, tmp_path: Path) -> None:
        path = str(tmp_path / 'plain.txt')
        task = _DownstreamTask(path=path, workspace_directory=str(tmp_path))
        assert task._get_required_task_outputs() == RequiredTaskOutput(task_name=f'{__name__}._LocalTargetTask', output_path=path)


class TestTaskInfoTree:
    def test_output_paths_of_local_target(self, tmp_path: Path) -> None:
        path = str(tmp_path / 'plain.txt')
        task = _DownstreamTask(path=path, workspace_directory=str(tmp_path))
        assert make_task_info_tree(task).children_task_infos[0].output_paths == [path]


class TestTaskLog:
    def test_file_path_of_local_target(self, tmp_path: Path) -> None:
        path = str(tmp_path / 'plain.txt')
        task = _LocalTargetTask(path=path, workspace_directory=str(tmp_path))
        task._dump_task_log()
        assert task.task_log['file_path'] == [path]

    def test_dumped_through_build(self, tmp_path: Path) -> None:
        # The task log is dumped by luigi's SUCCESS event handler, which swallows errors, so check the dumped file itself.
        path = str(tmp_path / 'plain.txt')
        task = _LocalTargetTask(path=path, workspace_directory=str(tmp_path / 'resources'))
        gokart.build(task, reset_register=False, return_value=False)
        assert task.load(task._get_task_log_target())['file_path'] == [path]
