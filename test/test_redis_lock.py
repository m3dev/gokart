import unittest

from gokart.redis_lock import RedisParams, make_redis_key, make_redis_params


class TestMakeRedisKey(unittest.TestCase):
    def test_make_redis_key(self):
        result = make_redis_key(file_path='gs://test_ll/dir/fname.pkl', unique_id='12345')
        self.assertEqual(result, 'fname_12345')


class TestMakeRedisParams(unittest.TestCase):
    def test_make_redis_params_with_valid_host(self):
        result = make_redis_params(file_path='gs://aaa.pkl', unique_id='123', redis_host='0.0.0.0', redis_port='12345')
        expected = RedisParams(redis_host='0.0.0.0', redis_port='12345', redis_key='aaa_123', should_redis_lock=True)
        self.assertEqual(result, expected)

    def test_make_redis_params_with_no_host(self):
        result = make_redis_params(file_path='gs://aaa.pkl', unique_id='123', redis_host=None, redis_port='12345')
        expected = RedisParams(redis_host=None, redis_port='12345', redis_key='aaa_123', should_redis_lock=False)
        self.assertEqual(result, expected)
