import os
import threading
import uuid
from collections.abc import Iterator
from unittest.mock import Mock, patch

import fakeredis
import luigi
import luigi.worker
import pytest
from luigi import scheduler
from redis.lock import Lock

import gokart
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params_for_run
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
            worker._check_complete_value('not a bool')  # pyright: ignore[reportArgumentType]


class _RunLockTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    random_id: luigi.StrParameter = luigi.StrParameter()
    fixed_priority: luigi.FloatParameter = luigi.FloatParameter(default=0.0, significant=False)

    @property
    def priority(self) -> float:
        # NOTE: TaskOnKart's priority is random; fix it so that the scheduler hands out tasks in a deterministic order.
        return self.fixed_priority

    def _run(self): ...

    def run(self):
        self._run()
        self.dump('test')

    def _complete(self) -> bool:
        return super().complete()

    def complete(self) -> bool:
        # NOTE: `complete_check_at_run` binds `self.complete` at `__init__`, so tests patch `_complete` instead.
        return self._complete()


@pytest.fixture
def redis_server() -> Iterator[fakeredis.FakeServer]:
    server = fakeredis.FakeServer()
    with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
        redis_mock.side_effect = lambda host, port: fakeredis.FakeRedis(server=server, host=host, port=port)
        yield server


def _make_run_lock_task(priority: float = 0.0) -> _RunLockTask:
    return _RunLockTask(random_id=uuid.uuid4().hex, fixed_priority=priority, redis_host='0.0.0.0', redis_port=12345, redis_timeout=180, should_lock_run=True)


def _run_lock_key(task: gokart.TaskOnKart[str]) -> str:
    return make_task_lock_params_for_run(task_self=task).redis_key


def _hold_run_lock(server: fakeredis.FakeServer, task: gokart.TaskOnKart[str]) -> Lock:
    lock = Lock(fakeredis.FakeRedis(server=server), name=_run_lock_key(task), timeout=180, thread_local=False)
    assert lock.acquire(blocking=False)
    return lock


def _fast_retry_config(force_multiprocessing: bool = False) -> gokart_worker:
    return gokart_worker(wait_interval=0.01, wait_jitter=0.0, force_multiprocessing=force_multiprocessing)


class TestWorkerRunLock:
    @pytest.mark.parametrize('fails', [pytest.param(False, id='succeeded'), pytest.param(True, id='failed')])
    def test_lock_is_released_after_task_finishes(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer, fails: bool) -> None:
        task = _make_run_lock_task()
        held_during_run: list[bool] = []

        def _run() -> None:
            held_during_run.append(bool(fakeredis.FakeRedis(server=redis_server).exists(_run_lock_key(task))))
            if fails:
                raise RuntimeError('failed')

        monkeypatch.setattr(task, '_run', _run)
        with Worker(scheduler=scheduler.Scheduler()) as worker:
            assert worker.add(task)
            assert worker.run() is not fails

        assert held_during_run == [True]
        assert not fakeredis.FakeRedis(server=redis_server).exists(_run_lock_key(task))

    def test_completion_is_checked_after_lock_is_acquired(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        task = _make_run_lock_task()
        lock_held_at_check: list[bool] = []

        def _complete() -> bool:
            lock_held_at_check.append(bool(fakeredis.FakeRedis(server=redis_server).exists(_run_lock_key(task))))
            return False

        with Worker(scheduler=scheduler.Scheduler()) as worker:
            assert worker.add(task)
            # NOTE: mock after `add` because `add` checks completion before any lock is taken.
            monkeypatch.setattr(task, '_complete', _complete)
            assert worker.run()

        # Both the worker's check and the task's `complete_check_at_run` check happen inside the lock.
        assert lock_held_at_check == [True, True]

    def test_locked_task_is_parked_and_other_task_runs_first(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        # The scheduler hands out the locked task first.
        locked_task = _make_run_lock_task(priority=1.0)
        other_task = _make_run_lock_task()
        external_lock = _hold_run_lock(redis_server, locked_task)
        run_order: list[str] = []

        def _run_other() -> None:
            run_order.append('other')
            external_lock.release()

        monkeypatch.setattr(locked_task, '_run', lambda: run_order.append('locked'))
        monkeypatch.setattr(other_task, '_run', _run_other)
        with Worker(scheduler=scheduler.Scheduler(), config=_fast_retry_config()) as worker:
            assert worker.add(locked_task)
            assert worker.add(other_task)
            assert worker.run()

        assert run_order == ['other', 'locked']

    def test_waits_for_lock_and_skips_task_completed_by_another_worker(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        task = _make_run_lock_task()
        external_lock = _hold_run_lock(redis_server, task)
        completed_elsewhere = threading.Event()

        def _finish_elsewhere() -> None:
            completed_elsewhere.set()
            external_lock.release()

        mock_run = Mock()
        monkeypatch.setattr(task, '_run', mock_run)
        with Worker(scheduler=scheduler.Scheduler(), config=_fast_retry_config()) as worker:
            assert worker.add(task)
            monkeypatch.setattr(task, '_complete', completed_elsewhere.is_set)
            timer = threading.Timer(0.1, _finish_elsewhere)
            timer.start()
            try:
                assert worker.run()
            finally:
                timer.join()

        mock_run.assert_not_called()

    def test_lock_is_released_when_task_process_dies(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        task = _make_run_lock_task()
        monkeypatch.setattr(task, '_run', lambda: os._exit(1))
        with Worker(scheduler=scheduler.Scheduler(), config=_fast_retry_config(force_multiprocessing=True)) as worker:
            assert worker.add(task)
            assert not worker.run()

        assert not fakeredis.FakeRedis(server=redis_server).exists(_run_lock_key(task))

    def test_lock_is_released_when_worker_exits_with_running_task(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        task = _make_run_lock_task()

        def _interrupt() -> None:
            raise KeyboardInterrupt

        monkeypatch.setattr(task, '_run', _interrupt)
        with pytest.raises(KeyboardInterrupt):
            with Worker(scheduler=scheduler.Scheduler()) as worker:
                assert worker.add(task)
                worker.run()

        assert not fakeredis.FakeRedis(server=redis_server).exists(_run_lock_key(task))

    def test_parked_task_still_runs_when_worker_phases_out(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        # The scheduler hands out the locked task first.
        locked_task = _make_run_lock_task(priority=1.0)
        other_task = _make_run_lock_task()
        external_lock = _hold_run_lock(redis_server, locked_task)
        run_order: list[str] = []

        with Worker(scheduler=scheduler.Scheduler(), config=_fast_retry_config()) as worker:

            def _run_other() -> None:
                run_order.append('other')
                # NOTE: SIGUSR1 makes the worker phase out; call the handler's target directly to do it while the locked task is parked.
                worker._start_phasing_out()
                external_lock.release()

            monkeypatch.setattr(locked_task, '_run', lambda: run_order.append('locked'))
            monkeypatch.setattr(other_task, '_run', _run_other)
            assert worker.add(locked_task)
            assert worker.add(other_task)
            assert worker.run()

        assert run_order == ['other', 'locked']

    def test_task_fails_without_crashing_worker_when_redis_is_unavailable(self, monkeypatch: pytest.MonkeyPatch, redis_server: fakeredis.FakeServer) -> None:
        task = _make_run_lock_task()
        mock_run = Mock()
        monkeypatch.setattr(task, '_run', mock_run)
        with Worker(scheduler=scheduler.Scheduler(), config=_fast_retry_config()) as worker:
            assert worker.add(task)
            redis_server.connected = False
            assert not worker.run()

        mock_run.assert_not_called()

    def test_retry_backoff_does_not_overflow_after_many_retries(self) -> None:
        with Worker(scheduler=scheduler.Scheduler()) as worker:
            waiter = worker._next_run_lock_waiter(Mock(), retry_count=2000)

        assert waiter.retry_count == 2000
