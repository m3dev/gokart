import random
import unittest
from unittest.mock import patch

from gokart.redis_lock import RedisClient, RedisParams, make_redis_key, make_redis_params


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


class TestMakeRedisKey(unittest.TestCase):
    def test_make_redis_key(self):
        result = make_redis_key(file_path='gs://test_ll/dir/fname.pkl', unique_id='12345')
        self.assertEqual(result, 'fname_12345')


class TestMakeRedisParams(unittest.TestCase):
    def test_make_redis_params_with_valid_host(self):
        result = make_redis_params(file_path='gs://aaa.pkl', unique_id='123', redis_host='0.0.0.0', redis_port='12345', redis_timeout=180)
        expected = RedisParams(redis_host='0.0.0.0', redis_port='12345', redis_key='aaa_123', should_redis_lock=True, redis_timeout=180)
        self.assertEqual(result, expected)

    def test_make_redis_params_with_no_host(self):
        result = make_redis_params(file_path='gs://aaa.pkl', unique_id='123', redis_host=None, redis_port='12345', redis_timeout=180)
        expected = RedisParams(redis_host=None, redis_port='12345', redis_key='aaa_123', should_redis_lock=False, redis_timeout=180)
        self.assertEqual(result, expected)
