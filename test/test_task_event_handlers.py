"""TaskOnKart's event handlers must run only for TaskOnKart, not for every luigi.Task in a process that imports gokart."""

import logging
from pathlib import Path
from typing import cast

import luigi
import pytest
from luigi.execution_summary import LuigiRunResult, LuigiStatusCode

import gokart
from gokart.errors import GokartBuildError


class _PlainTask(luigi.Task):
    task_namespace = __name__
    path: luigi.Parameter[str] = luigi.Parameter()

    def output(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        return luigi.LocalTarget(self.path)

    def run(self):
        with self.output().open('w') as f:
            f.write('ok')


class _FailingPlainTask(luigi.Task):
    task_namespace = __name__

    def complete(self):
        return False

    def run(self):
        raise RuntimeError('boom')


class _KartTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def run(self):
        self.dump('done')


class _FailingKartTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def run(self):
        raise RuntimeError('boom')


def _event_callback_errors(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.ERROR and 'Error in event callback' in r.getMessage()]


def test_plain_luigi_task_success_does_not_log_event_callback_errors(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR, logger='luigi-interface'):
        assert luigi.build([_PlainTask(path=str(tmp_path / 'plain.txt'))], local_scheduler=True)

    assert _event_callback_errors(caplog) == []


def test_plain_luigi_task_failure_does_not_log_event_callback_errors(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.ERROR, logger='luigi-interface'):
        # WHY: a scheduling failure also makes luigi.build fail, but then the task never runs and FAILURE handlers are never triggered.
        result = cast(LuigiRunResult, luigi.build([_FailingPlainTask()], local_scheduler=True, detailed_summary=True))
    assert result.status == LuigiStatusCode.FAILED

    assert _event_callback_errors(caplog) == []


def test_task_on_kart_success_runs_its_event_handlers(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    task = _KartTask(workspace_directory=str(tmp_path))
    with caplog.at_level(logging.ERROR, logger='luigi-interface'):
        assert gokart.build(task, reset_register=False) == 'done'

    assert _event_callback_errors(caplog) == []
    assert task._get_random_seeds_target().exists()
    assert task._get_task_params_target().exists()
    assert task._get_module_versions_target().exists()
    assert task._get_processing_time_target().exists()
    assert task._get_task_log_target().exists()


def test_task_on_kart_failure_logs_its_unique_id(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    task = _FailingKartTask(workspace_directory=str(tmp_path))
    with caplog.at_level(logging.INFO, logger='gokart.task'), pytest.raises(GokartBuildError):
        gokart.build(task, reset_register=False, log_level=logging.INFO)

    assert f'unique id={task.make_unique_id()}' in caplog.text
    assert _event_callback_errors(caplog) == []
