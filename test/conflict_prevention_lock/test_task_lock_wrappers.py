import time
import unittest
from unittest.mock import MagicMock, patch

import fakeredis

from gokart.conflict_prevention_lock.task_lock import make_task_lock_params
from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_dump_with_lock, wrap_load_with_lock, wrap_remove_with_lock


def _sample_func_with_error(a: int, b: str):
    raise Exception()


def _sample_long_func(a: int, b: str):
    time.sleep(2.7)
    return dict(a=a, b=b)


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
        self.assertDictEqual(called_kwargs, dict(b='abc'))

    def test_use_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            wrap_dump_with_lock(func=mock_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, dict(b='abc'))

    def test_if_func_is_skipped_when_cache_already_exists(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
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

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            wrap_dump_with_lock(func=_sample_long_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            mock_func = MagicMock()
            wrap_dump_with_lock(func=mock_func, task_lock_params=task_lock_params, exist_check=lambda: False)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, dict(b='abc'))

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

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
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
        self.assertDictEqual(called_kwargs, dict(b='abc'))

        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = wrap_load_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, dict(b='abc'))

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

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_load_with_lock(func=_sample_long_func, task_lock_params=task_lock_params)(123, b='abc')
            expected = dict(a=123, b='abc')
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_load_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
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

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
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
        self.assertDictEqual(called_kwargs, dict(b='abc'))
        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = wrap_remove_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
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

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_remove_with_lock(func=_sample_long_func, task_lock_params=task_lock_params)(123, b='abc')
            expected = dict(a=123, b='abc')
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        task_lock_params = make_task_lock_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_remove_with_lock(func=mock_func, task_lock_params=task_lock_params)(123, b='abc')
            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123,))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
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

        with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=task_lock_params.redis_host, port=task_lock_params.redis_port)
            try:
                wrap_remove_with_lock(func=_sample_func_with_error, task_lock_params=task_lock_params)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[task_lock_params.redis_key]
