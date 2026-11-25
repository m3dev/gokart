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
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params
from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_dump_with_lock, wrap_load_with_lock, wrap_remove_with_lock, wrap_run_with_lock


def _sample_func_with_error(a: int, b: str) -> None:
    raise Exception()


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

    def long_func(self, a: int, b: str) -> dict[str, int | str]:
        assert self._extended.wait(timeout=10), 'lock was not extended'
        return {'a': a, 'b': b}


class TestWrapDumpWithLock(unittest.TestCase):
    def test_no_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        wrap_dump_with_lock(func=mock_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123,))
        self.assertDictEqual(called_kwargs, {'b': 'abc'})

    def test_use_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            wrap_dump_with_lock(func=mock_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, {'b': 'abc'})

    def test_if_func_is_skipped_when_cache_already_exists(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            wrap_dump_with_lock(func=mock_func, task_lock_params=task_lock_params, exist_check=lambda: True)(123, b='abc')

            mock_func.assert_not_called()

    def test_check_lock_extended(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        probe = _LockExtensionProbe()
        with probe.patch_extend(), patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            wrap_dump_with_lock(func=probe.long_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            mock_func = MagicMock()
            wrap_dump_with_lock(func=mock_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, {'b': 'abc'})

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[task_lock_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            try:
                wrap_dump_with_lock(func=_sample_func_with_error, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[task_lock_params.redis_key]


class TestWrapLoadWithLock(unittest.TestCase):
    def test_no_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        resulted = wrap_load_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123,))
        self.assertDictEqual(called_kwargs, {'b': 'abc'})

        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = wrap_load_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, {'b': 'abc'})

            self.assertEqual(resulted, mock_func())

    def test_lock_is_released_before_func_is_called(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        server = fakeredis.FakeServer()
        fake_redis = fakeredis.FakeStrictRedis(server=server)

        def func_checking_lock(a: int, b: str) -> dict[str, int | str]:
            self.assertFalse(fake_redis.exists(task_lock_params.redis_key))
            return {'a': a, 'b': b}

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            resulted = wrap_load_with_lock(func=func_checking_lock, task_lock_params=task_lock_params)(123, b='abc')
            expected = {'a': 123, 'b': 'abc'}
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_load_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, {'b': 'abc'})
            self.assertEqual(resulted, mock_func())

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[task_lock_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            try:
                wrap_load_with_lock(func=_sample_func_with_error, task_lock_params=task_lock_params)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[task_lock_params.redis_key]


class TestWrapRemoveWithLock(unittest.TestCase):
    def test_no_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        resulted = wrap_remove_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123,))
        self.assertDictEqual(called_kwargs, {'b': 'abc'})
        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = wrap_remove_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, {'b': 'abc'})
            self.assertEqual(resulted, mock_func())

    def test_check_lock_extended(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        probe = _LockExtensionProbe()
        with probe.patch_extend(), patch('redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_remove_with_lock(func=probe.long_func, task_lock_params=task_lock_params)(123, b='abc')
            expected = {'a': 123, 'b': 'abc'}
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_remove_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')
            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, {'b': 'abc'})
            self.assertEqual(resulted, mock_func())

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[task_lock_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            try:
                wrap_remove_with_lock(func=_sample_func_with_error, task_lock_params=task_lock_params)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[task_lock_params.redis_key]


class TestWrapRunWithLock(unittest.TestCase):
    def test_is_deprecated(self):
        task_lock_params = make_task_lock_params(file_path='test_dir/test_file.pkl', unique_id='123abc')
        with self.assertWarnsRegex(FutureWarning, r'wrap_run_with_lock is deprecated and will be removed in gokart 2\.0\.0'):
            wrap_run_with_lock(run_func=MagicMock(), task_lock_params=task_lock_params)


def _wrap_run_without_warning(func: Callable[[], Any], task_lock_params) -> Callable[[], Any]:
    with warnings.catch_warnings():
        warnings.simplefilter('ignore', FutureWarning)
        return wrap_run_with_lock(run_func=func, task_lock_params=task_lock_params)


_WRAPPERS: dict[str, Callable[[Callable[[], Any], Any], Callable[[], Any]]] = {
    'dump': lambda func, params: wrap_dump_with_lock(func=func, task_lock_params=params, exist_check=lambda: False),
    'load': lambda func, params: wrap_load_with_lock(func=func, task_lock_params=params),
    'remove': lambda func, params: wrap_remove_with_lock(func=func, task_lock_params=params),
    'run': _wrap_run_without_warning,
}


def _lock_scheduler_threads() -> list[threading.Thread]:
    return [t for t in threading.enumerate() if t.name == 'APScheduler']


class TestLockCleanupWhenUnlockingFails(unittest.TestCase):
    """The scheduler extending the lock must always be shut down and the lock released exactly once, even if one of them raises.

    https://github.com/m3dev/gokart/pull/705
    """

    def _task_lock_params(self, wrapper_name: str):
        # RedisClient caches one fake Redis per host and port across tests, so a lock left behind under a shared key would block other tests.
        return make_task_lock_params(file_path='test_dir/test_file.pkl', unique_id=f'{self.id()}-{wrapper_name}', redis_host='0.0.0.0', redis_port=12345)

    def test_scheduler_is_shut_down_and_lock_released_once_when_release_fails(self):
        for name, wrap in _WRAPPERS.items():
            with self.subTest(wrapper=name):
                threads_before = _lock_scheduler_threads()
                with (
                    patch('redis.Redis', side_effect=fakeredis.FakeRedis),
                    patch.object(Lock, 'release', autospec=True, side_effect=LockNotOwnedError('lock expired')) as release_mock,
                ):
                    with self.assertRaises(LockNotOwnedError):
                        wrap(MagicMock(), self._task_lock_params(name))()

                self.assertEqual(release_mock.call_count, 1)
                self.assertEqual(_lock_scheduler_threads(), threads_before)

    def test_func_error_is_not_hidden_by_release_failure(self):
        # wrap_load_with_lock releases the lock before calling the function.
        for name in ('dump', 'remove', 'run'):
            with self.subTest(wrapper=name):
                threads_before = _lock_scheduler_threads()
                with (
                    patch('redis.Redis', side_effect=fakeredis.FakeRedis),
                    patch.object(Lock, 'release', autospec=True, side_effect=LockNotOwnedError('lock expired')) as release_mock,
                ):
                    with self.assertRaises(ValueError):
                        _WRAPPERS[name](MagicMock(side_effect=ValueError('func failed')), self._task_lock_params(name))()

                self.assertEqual(release_mock.call_count, 1)
                self.assertEqual(_lock_scheduler_threads(), threads_before)

    def test_lock_is_released_once_when_scheduler_shutdown_fails(self):
        real_shutdown = BackgroundScheduler.shutdown

        def shutdown_then_fail(scheduler: BackgroundScheduler, *args: Any, **kwargs: Any) -> None:
            real_shutdown(scheduler, *args, **kwargs)
            raise RuntimeError('shutdown failed')

        for name, wrap in _WRAPPERS.items():
            with self.subTest(wrapper=name):
                with (
                    patch('redis.Redis', side_effect=fakeredis.FakeRedis),
                    patch.object(BackgroundScheduler, 'shutdown', autospec=True, side_effect=shutdown_then_fail),
                    patch.object(Lock, 'release', autospec=True, side_effect=Lock.release) as release_mock,
                ):
                    with self.assertRaises(RuntimeError):
                        wrap(MagicMock(), self._task_lock_params(name))()

                self.assertEqual(release_mock.call_count, 1)

    def test_lock_is_released_when_scheduler_fails_to_start(self):
        for name, wrap in _WRAPPERS.items():
            with self.subTest(wrapper=name):
                func = MagicMock()
                with (
                    patch('redis.Redis', side_effect=fakeredis.FakeRedis),
                    patch.object(BackgroundScheduler, 'start', autospec=True, side_effect=RuntimeError("can't start new thread")),
                    patch.object(Lock, 'release', autospec=True, side_effect=Lock.release) as release_mock,
                ):
                    with self.assertRaises(RuntimeError):
                        wrap(func, self._task_lock_params(name))()

                self.assertEqual(release_mock.call_count, 1)
                func.assert_not_called()
