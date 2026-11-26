import threading
import unittest
import warnings
from collections.abc import Callable
from typing import Any
from unittest.mock import MagicMock, patch

import fakeredis
from apscheduler.schedulers.background import BackgroundScheduler  # pyright: ignore[reportMissingTypeStubs]
from redis.exceptions import LockNotOwnedError
from redis.lock import Lock

from gokart.conflict_prevention_lock import task_lock as task_lock_module
from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_run_with_lock


def _task_lock_params(redis_timeout: int = 180, lock_extend_seconds: int = 10) -> TaskLockParams:
    return TaskLockParams(
        redis_host='0.0.0.0',
        redis_port=12345,
        redis_timeout=redis_timeout,
        redis_key='test_file_123abc',
        should_task_lock=True,
        raise_task_lock_exception_on_collision=False,
        lock_extend_seconds=lock_extend_seconds,
    )


def _wrap_run(run_func: Callable[[], Any], task_lock_params: TaskLockParams) -> Callable[[], Any]:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', FutureWarning)
        return wrap_run_with_lock(run_func=run_func, task_lock_params=task_lock_params)


def _sample_func_with_error() -> None:
    raise ValueError('run failed')


class _LockExtensionProbe:
    """Blocks a locked function until the lock scheduler extends the lock.

    Waiting on the extension itself, rather than sleeping past `redis_timeout`, keeps the test free of wall-clock assumptions.
    """

    def __init__(self) -> None:
        self._extended = threading.Event()
        self._real_extend = task_lock_module._extend_lock

    def _extend(self, task_lock, redis_timeout: int) -> None:
        self._real_extend(task_lock=task_lock, redis_timeout=redis_timeout)
        self._extended.set()

    def patch_extend(self):
        return patch.object(task_lock_module, '_extend_lock', side_effect=self._extend)

    def long_func(self) -> str:
        assert self._extended.wait(timeout=10), 'lock was not extended'
        return 'done'


class TestWrapRunWithLock(unittest.TestCase):
    def test_is_deprecated(self):
        with self.assertWarnsRegex(FutureWarning, r'wrap_run_with_lock is deprecated and will be removed in gokart 2\.0\.0'):
            wrap_run_with_lock(run_func=MagicMock(), task_lock_params=_task_lock_params())

    def test_use_redis(self):
        with patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = _wrap_run(mock_func, _task_lock_params())()

            mock_func.assert_called_once_with()
            self.assertEqual(resulted, mock_func())

    def test_check_lock_extended(self):
        probe = _LockExtensionProbe()
        with probe.patch_extend(), patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = _wrap_run(probe.long_func, _task_lock_params(redis_timeout=2, lock_extend_seconds=1))()
            self.assertEqual(resulted, 'done')

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = _task_lock_params()
        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            _wrap_run(MagicMock(), task_lock_params)()

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[task_lock_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        task_lock_params = _task_lock_params()
        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            with self.assertRaises(ValueError):
                _wrap_run(_sample_func_with_error, task_lock_params)()

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[task_lock_params.redis_key]


def _lock_scheduler_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == 'APScheduler']


class TestLockCleanupWhenUnlockingFails(unittest.TestCase):
    """The scheduler extending the lock must always be shut down and the lock released exactly once, even if one of them raises.

    https://github.com/m3dev/gokart/issues/709
    """

    def _task_lock_params(self) -> TaskLockParams:
        # RedisClient caches one fake Redis per host and port across tests, so a lock left behind under a shared key would block other tests.
        return _task_lock_params()._replace(redis_key=self.id())

    def test_scheduler_is_shut_down_and_lock_released_once_when_release_fails(self):
        threads_before = _lock_scheduler_threads()
        with (
            patch('redis.Redis', side_effect=fakeredis.FakeRedis),
            patch.object(Lock, 'release', autospec=True, side_effect=LockNotOwnedError('lock expired')) as release_mock,
            self.assertRaises(LockNotOwnedError),
        ):
            _wrap_run(MagicMock(), self._task_lock_params())()

        self.assertEqual(release_mock.call_count, 1)
        self.assertEqual(_lock_scheduler_threads(), threads_before)

    def test_func_error_is_not_hidden_by_release_failure(self):
        threads_before = _lock_scheduler_threads()
        with (
            patch('redis.Redis', side_effect=fakeredis.FakeRedis),
            patch.object(Lock, 'release', autospec=True, side_effect=LockNotOwnedError('lock expired')) as release_mock,
            self.assertRaises(ValueError),
        ):
            _wrap_run(MagicMock(side_effect=ValueError('func failed')), self._task_lock_params())()

        self.assertEqual(release_mock.call_count, 1)
        self.assertEqual(_lock_scheduler_threads(), threads_before)

    def test_lock_is_released_once_when_scheduler_shutdown_fails(self):
        real_shutdown = BackgroundScheduler.shutdown

        def shutdown_then_fail(scheduler: BackgroundScheduler, *args: Any, **kwargs: Any) -> None:
            real_shutdown(scheduler, *args, **kwargs)
            raise RuntimeError('shutdown failed')

        with (
            patch('redis.Redis', side_effect=fakeredis.FakeRedis),
            patch.object(BackgroundScheduler, 'shutdown', autospec=True, side_effect=shutdown_then_fail),
            patch.object(Lock, 'release', autospec=True, side_effect=Lock.release) as release_mock,
            self.assertRaises(RuntimeError),
        ):
            _wrap_run(MagicMock(), self._task_lock_params())()

        self.assertEqual(release_mock.call_count, 1)

    def test_lock_is_released_when_scheduler_fails_to_start(self):
        func = MagicMock()
        with (
            patch('redis.Redis', side_effect=fakeredis.FakeRedis),
            patch.object(BackgroundScheduler, 'start', autospec=True, side_effect=RuntimeError("can't start new thread")),
            patch.object(Lock, 'release', autospec=True, side_effect=Lock.release) as release_mock,
            self.assertRaises(RuntimeError),
        ):
            _wrap_run(func, self._task_lock_params())()

        self.assertEqual(release_mock.call_count, 1)
        func.assert_not_called()
