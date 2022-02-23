import random
import time
import unittest
from unittest.mock import MagicMock, patch

import fakeredis

from gokart.redis_lock import RedisClient, RedisParams, make_redis_key, make_redis_params, wrap_with_dump_lock, wrap_with_remove_lock, wrap_with_run_lock


class TestRedisClient(unittest.TestCase):

    @staticmethod
    def _get_randint(host, port):
        return random.randint(0, 100000)

    def test_redis_client_is_singleton(self):
        with patch('redis.Redis') as mock:
            mock.side_effect = self._get_randint

            redis_client_0_0 = RedisClient(host='host_0', port='123')
            redis_client_1 = RedisClient(host='host_1', port='123')
            redis_client_0_1 = RedisClient(host='host_0', port='123')

            self.assertNotEqual(redis_client_0_0, redis_client_1)
            self.assertEqual(redis_client_0_0, redis_client_0_1)

            self.assertEqual(redis_client_0_0.get_redis_client(), redis_client_0_1.get_redis_client())


def _sample_func_with_error(a: int, b: str = None):
    raise Exception()


def _sample_long_func(a: int, b: str = None):
    time.sleep(3)
    return dict(a=a, b=b)


class TestWrapWithRunLock(unittest.TestCase):

    def test_no_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        resulted = wrap_with_run_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123, ))
        self.assertDictEqual(called_kwargs, dict(b='abc'))
        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            mock_func = MagicMock()
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_with_run_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
            self.assertEqual(resulted, mock_func())

    def test_check_lock_extended(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_with_run_lock(func=_sample_long_func, redis_params=redis_params)(123, b='abc')
            expected = dict(a=123, b='abc')
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_with_run_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
            self.assertEqual(resulted, mock_func())

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[redis_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            try:
                wrap_with_run_lock(func=_sample_func_with_error, redis_params=redis_params)(a=123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[redis_params.redis_key]


class TestWrapWithDumpLock(unittest.TestCase):

    def test_no_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        wrap_with_dump_lock(func=mock_func, redis_params=redis_params, exist_check=lambda: False)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123, ))
        self.assertDictEqual(called_kwargs, dict(b='abc'))

    def test_use_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            wrap_with_dump_lock(func=mock_func, redis_params=redis_params, exist_check=lambda: False)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))

    def test_if_func_is_skipped_when_cache_already_exists(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            wrap_with_dump_lock(func=mock_func, redis_params=redis_params, exist_check=lambda: True)(123, b='abc')

            mock_func.assert_not_called()

    def test_check_lock_extended(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            wrap_with_dump_lock(func=_sample_long_func, redis_params=redis_params, exist_check=lambda: False)(123, b='abc')

    def test_lock_is_removed_after_func_is_finished(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            mock_func = MagicMock()
            wrap_with_dump_lock(func=mock_func, redis_params=redis_params, exist_check=lambda: False)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[redis_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            try:
                wrap_with_dump_lock(func=_sample_func_with_error, redis_params=redis_params, exist_check=lambda: False)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[redis_params.redis_key]


class TestWrapWithLoadLock(unittest.TestCase):

    def test_no_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        resulted = wrap_with_run_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123, ))
        self.assertDictEqual(called_kwargs, dict(b='abc'))

        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = wrap_with_run_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))

            self.assertEqual(resulted, mock_func())

    def test_check_lock_extended(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_with_run_lock(func=_sample_long_func, redis_params=redis_params)(123, b='abc')
            expected = dict(a=123, b='abc')
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_with_run_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
            self.assertEqual(resulted, mock_func())

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[redis_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            try:
                wrap_with_run_lock(func=_sample_func_with_error, redis_params=redis_params)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[redis_params.redis_key]


class TestWrapWithRemoveLock(unittest.TestCase):

    def test_no_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host=None,
            redis_port=None,
        )
        mock_func = MagicMock()
        resulted = wrap_with_remove_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

        mock_func.assert_called_once()
        called_args, called_kwargs = mock_func.call_args
        self.assertTupleEqual(called_args, (123, ))
        self.assertDictEqual(called_kwargs, dict(b='abc'))
        self.assertEqual(resulted, mock_func())

    def test_use_redis(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            mock_func = MagicMock()
            resulted = wrap_with_remove_lock(func=mock_func, redis_params=redis_params)(123, b='abc')

            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
            self.assertEqual(resulted, mock_func())

    def test_check_lock_extended(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=2,
            lock_extend_seconds=1,
        )

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.side_effect = fakeredis.FakeRedis
            resulted = wrap_with_remove_lock(func=_sample_long_func, redis_params=redis_params)(123, b='abc')
            expected = dict(a=123, b='abc')
            self.assertEqual(resulted, expected)

    def test_lock_is_removed_after_func_is_finished(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            mock_func = MagicMock()
            resulted = wrap_with_remove_lock(func=mock_func, redis_params=redis_params)(123, b='abc')
            mock_func.assert_called_once()
            called_args, called_kwargs = mock_func.call_args
            self.assertTupleEqual(called_args, (123, ))
            self.assertDictEqual(called_kwargs, dict(b='abc'))
            self.assertEqual(resulted, mock_func())

            fake_redis = fakeredis.FakeStrictRedis(server=server)
            with self.assertRaises(KeyError):
                fake_redis[redis_params.redis_key]

    def test_lock_is_removed_after_func_is_finished_with_error(self):
        redis_params = make_redis_params(
            file_path='test_dir/test_file.pkl',
            unique_id='123abc',
            redis_host='0.0.0.0',
            redis_port=12345,
        )

        server = fakeredis.FakeServer()

        with patch('gokart.redis_lock.redis.Redis') as redis_mock:
            redis_mock.return_value = fakeredis.FakeRedis(server=server, host=redis_params.redis_host, port=redis_params.redis_port)
            try:
                wrap_with_remove_lock(func=_sample_func_with_error, redis_params=redis_params)(123, b='abc')
            except Exception:
                fake_redis = fakeredis.FakeStrictRedis(server=server)
                with self.assertRaises(KeyError):
                    fake_redis[redis_params.redis_key]


class TestMakeRedisKey(unittest.TestCase):

    def test_make_redis_key(self):
        result = make_redis_key(file_path='gs://test_ll/dir/fname.pkl', unique_id='12345')
        self.assertEqual(result, 'fname_12345')


class TestMakeRedisParams(unittest.TestCase):

    def test_make_redis_params_with_valid_host(self):
        result = make_redis_params(file_path='gs://aaa.pkl',
                                   unique_id='123',
                                   redis_host='0.0.0.0',
                                   redis_port='12345',
                                   redis_timeout=180,
                                   redis_fail_on_collision=False)
        expected = RedisParams(redis_host='0.0.0.0',
                               redis_port='12345',
                               redis_key='aaa_123',
                               should_redis_lock=True,
                               redis_timeout=180,
                               redis_fail_on_collision=False,
                               lock_extend_seconds=10)
        self.assertEqual(result, expected)

    def test_make_redis_params_with_no_host(self):
        result = make_redis_params(file_path='gs://aaa.pkl',
                                   unique_id='123',
                                   redis_host=None,
                                   redis_port='12345',
                                   redis_timeout=180,
                                   redis_fail_on_collision=False)
        expected = RedisParams(redis_host=None,
                               redis_port='12345',
                               redis_key='aaa_123',
                               should_redis_lock=False,
                               redis_timeout=180,
                               redis_fail_on_collision=False,
                               lock_extend_seconds=10)
        self.assertEqual(result, expected)

    def test_assert_when_redis_timeout_is_too_short(self):
        with self.assertRaises(AssertionError):
            make_redis_params(
                file_path='test_dir/test_file.pkl',
                unique_id='123abc',
                redis_host='0.0.0.0',
                redis_port=12345,
                redis_timeout=2,
            )
