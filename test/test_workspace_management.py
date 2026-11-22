import os
from pathlib import Path

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
    upstream: gokart.TaskInstanceParameter[_UpstreamTask] = gokart.TaskInstanceParameter()

    def requires(self):
        return self.upstream

    def output(self) -> TargetOnKart:
        return self.make_target('downstream.pkl')

    def run(self):
        self.dump('downstream')


def _build(workspace: str, upstream_workspace: str | None = None) -> tuple[_DownstreamTask, list[str]]:
    upstream = _UpstreamTask(workspace_directory=upstream_workspace or workspace)
    task = _DownstreamTask(upstream=upstream, workspace_directory=workspace)
    gokart.build(task, reset_register=False, return_value=False)
    return task, [upstream.output().path(), task.output().path()]


class TestDeleteLocalUnnecessaryOutputs:
    # './resources/' is the default workspace_directory.
    @pytest.mark.parametrize('workspace', ['./resources/', './resources', 'resources/', 'sub/../resources'])
    def test_keeps_outputs_with_relative_workspace(self, tmp_path: Path, workspace: str) -> None:
        (tmp_path / 'sub').mkdir()
        task, outputs = _build(workspace)
        stale_file = Path(workspace) / 'stale.pkl'
        stale_file.write_text('stale')

        gokart.delete_local_unnecessary_outputs(task)

        assert all(os.path.exists(path) for path in outputs)
        assert not stale_file.exists()

    def test_keeps_output_written_through_symlinked_workspace(self, tmp_path: Path) -> None:
        real = tmp_path / 'real'
        real.mkdir()
        link = tmp_path / 'link'
        link.symlink_to(real, target_is_directory=True)
        task, outputs = _build(str(link), upstream_workspace=str(real))
        stale_file = link / 'stale.pkl'
        stale_file.write_text('stale')

        gokart.delete_local_unnecessary_outputs(task)

        assert all(os.path.exists(path) for path in outputs)
        assert not stale_file.exists()

    def test_keeps_output_of_task_whose_workspace_is_written_differently(self) -> None:
        task, outputs = _build('resources', upstream_workspace='./resources/')

        gokart.delete_local_unnecessary_outputs(task)

        assert all(os.path.exists(path) for path in outputs)

    def test_deletes_stale_symlink_to_needed_output(self) -> None:
        task, outputs = _build('resources')
        stale_link = Path('resources') / 'stale.pkl'
        stale_link.symlink_to(os.path.abspath(outputs[0]))

        gokart.delete_local_unnecessary_outputs(task)

        assert not stale_link.is_symlink()
        assert all(os.path.exists(path) for path in outputs)

    def test_skips_directory_whose_name_has_a_dot(self) -> None:
        task, outputs = _build('resources')
        dotted_directory = Path('resources') / 'data.v2'
        dotted_directory.mkdir()

        gokart.delete_local_unnecessary_outputs(task)

        assert dotted_directory.is_dir()
        assert all(os.path.exists(path) for path in outputs)
