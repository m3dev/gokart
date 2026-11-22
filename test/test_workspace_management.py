import os
from pathlib import Path

import luigi
import pytest

import gokart
from gokart.target import TargetOnKart


class _UpstreamTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def output(self) -> TargetOnKart:
        return self.make_target('upstream.pkl')

    def run(self):
        self.dump('upstream')


class _DownstreamTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    upstream_workspace_directory: luigi.OptionalParameter = luigi.OptionalParameter(default=None, significant=False)

    def requires(self):
        return _UpstreamTask(workspace_directory=self.upstream_workspace_directory or self.workspace_directory)

    def output(self) -> TargetOnKart:
        return self.make_target('downstream.pkl')

    def run(self):
        self.dump('downstream')


def _build_and_add_stale_file(workspace: str) -> tuple[_DownstreamTask, list[str], Path]:
    task = _DownstreamTask(workspace_directory=workspace)
    gokart.build(task, reset_register=False, return_value=False)
    outputs = [_UpstreamTask(workspace_directory=workspace).output().path(), task.output().path()]
    stale_file = Path(workspace) / 'stale.pkl'
    stale_file.write_text('stale')
    return task, outputs, stale_file


class TestDeleteLocalUnnecessaryOutputs:
    # './resources/' is the default workspace_directory.
    @pytest.mark.parametrize('workspace', ['./resources/', './resources', 'resources/', 'sub/../resources'])
    def test_keeps_outputs_with_relative_workspace(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workspace: str) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / 'sub').mkdir()
        task, outputs, stale_file = _build_and_add_stale_file(workspace)

        gokart.delete_local_unnecessary_outputs(task)

        assert all(os.path.exists(path) for path in outputs)
        assert not stale_file.exists()

    def test_keeps_outputs_with_symlinked_workspace(self, tmp_path: Path) -> None:
        real = tmp_path / 'real'
        real.mkdir()
        link = tmp_path / 'link'
        link.symlink_to(real, target_is_directory=True)
        task, outputs, stale_file = _build_and_add_stale_file(str(link))

        gokart.delete_local_unnecessary_outputs(task)

        assert all(os.path.exists(path) for path in outputs)
        assert not stale_file.exists()

    def test_keeps_output_of_task_whose_workspace_is_written_differently(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        task = _DownstreamTask(workspace_directory='resources', upstream_workspace_directory='./resources/')
        gokart.build(task, reset_register=False, return_value=False)
        upstream_output = _UpstreamTask(workspace_directory='./resources/').output().path()

        gokart.delete_local_unnecessary_outputs(task)

        assert os.path.exists(upstream_output)
