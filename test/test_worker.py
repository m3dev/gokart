import uuid
from unittest.mock import Mock

import luigi
import pytest
from luigi import scheduler

import gokart
from gokart.worker import Worker


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    random_id = luigi.Parameter()

    def _run(self): ...

    def run(self):
        self._run()
        self.dump('test')


class TestWorkerRun:
    def test_run(self, monkeypatch: pytest.MonkeyPatch):
        """Check run is called when the task is not completed"""
        sch = scheduler.Scheduler()
        worker = Worker(scheduler=sch)

        task = _DummyTask(random_id=uuid.uuid4().hex)
        mock_run = Mock()
        monkeypatch.setattr(task, '_run', mock_run)
        with worker:
            assert worker.add(task)
            assert worker.run()
            mock_run.assert_called_once()
