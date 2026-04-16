import uuid
from unittest.mock import Mock

import luigi
import luigi.worker
import pytest
from luigi import scheduler

import gokart
from gokart.worker import Worker, gokart_worker


class _DummyTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    random_id: luigi.StrParameter = luigi.StrParameter()

    def _run(self): ...

    def run(self):
        self._run()
        self.dump('test')


class TestWorkerRun:
    def test_run(self, monkeypatch: pytest.MonkeyPatch) -> None:
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


class _DummyTaskToCheckSkip(gokart.TaskOnKart[None]):
    task_namespace = __name__

    def _run(self): ...

    def run(self):
        self._run()
        self.dump(None)

    def complete(self) -> bool:
        return False


class TestWorkerSkipIfCompletedPreRun:
    @pytest.mark.parametrize(
        'task_completion_check_at_run,is_completed,expect_skipped',
        [
            pytest.param(True, True, True, id='skipped when completed and task_completion_check_at_run is True'),
            pytest.param(True, False, False, id='not skipped when not completed and task_completion_check_at_run is True'),
            pytest.param(False, True, False, id='not skipped when completed and task_completion_check_at_run is False'),
            pytest.param(False, False, False, id='not skipped when not completed and task_completion_check_at_run is False'),
        ],
    )
    def test_skip_task(self, monkeypatch: pytest.MonkeyPatch, task_completion_check_at_run: bool, is_completed: bool, expect_skipped: bool) -> None:
        sch = scheduler.Scheduler()
        worker = Worker(scheduler=sch, config=gokart_worker(task_completion_check_at_run=task_completion_check_at_run))

        mock_complete = Mock(return_value=is_completed)
        # NOTE: set `complete_check_at_run=False` to avoid using deprecated skip logic.
        task = _DummyTaskToCheckSkip(complete_check_at_run=False)
        mock_run = Mock()
        monkeypatch.setattr(task, '_run', mock_run)

        with worker:
            assert worker.add(task)
            # NOTE: mock `complete` after `add` because `add` calls `complete`
            #       to check if the task is already completed.
            monkeypatch.setattr(task, 'complete', mock_complete)
            assert worker.run()

            if expect_skipped:
                mock_run.assert_not_called()
            else:
                mock_run.assert_called_once()


class TestWorkerCheckCompleteValue:
    def test_does_not_raise_for_boolean_values(self) -> None:
        worker = Worker(scheduler=scheduler.Scheduler())
        worker._check_complete_value(True)
        worker._check_complete_value(False)

    def test_raises_async_completion_exception_for_traceback_wrapper(self) -> None:
        # NOTE: When Task.complete() raises in an async check, the exception is wrapped
        #       in TracebackWrapper. This branch must raise AsyncCompletionException.
        worker = Worker(scheduler=scheduler.Scheduler())
        wrapped = luigi.worker.TracebackWrapper(trace='dummy traceback')
        with pytest.raises(luigi.worker.AsyncCompletionException):
            worker._check_complete_value(wrapped)

    def test_raises_exception_for_non_boolean_value(self) -> None:
        # NOTE: Pass a non-bool value to verify the runtime guard against a misimplemented
        #       Task.complete() returning a non-boolean. The type ignore is intentional.
        worker = Worker(scheduler=scheduler.Scheduler())
        with pytest.raises(Exception, match='Return value of Task.complete'):
            worker._check_complete_value('not a bool')  # type: ignore[arg-type]
