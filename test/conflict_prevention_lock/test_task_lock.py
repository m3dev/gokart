import random
import unittest
from unittest.mock import patch

import gokart
from gokart.conflict_prevention_lock.task_lock import RedisClient, TaskLockParams, make_task_lock_key, make_task_lock_params, make_task_lock_params_for_run


class TestRedisClient(unittest.TestCase):
    @staticmethod
    def _get_randint(host, port):
        return random.randint(0, 100000)

    def test_redis_client_is_singleton(self):
        with patch('redis.Redis') as mock:
            mock.side_effect = self._get_randint

            redis_client_0_0 = RedisClient(host='host_0', port=123)
            redis_client_1 = RedisClient(host='host_1', port=123)
            redis_client_0_1 = RedisClient(host='host_0', port=123)

            self.assertNotEqual(redis_client_0_0, redis_client_1)
            self.assertEqual(redis_client_0_0, redis_client_0_1)

            self.assertEqual(redis_client_0_0.get_redis_client(), redis_client_0_1.get_redis_client())


class TestMakeRedisKey(unittest.TestCase):
    def test_make_redis_key(self):
        result = make_task_lock_key(file_path='gs://test_ll/dir/fname.pkl', unique_id='12345')
        self.assertEqual(result, 'fname_12345')


class TestMakeRedisParams(unittest.TestCase):
    def test_make_task_lock_params_with_valid_host(self):
        result = make_task_lock_params(
            file_path='gs://aaa.pkl', unique_id='123', redis_host='0.0.0.0', redis_port=12345, redis_timeout=180, raise_task_lock_exception_on_collision=False
        )
        expected = TaskLockParams(
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_key='aaa_123',
            should_task_lock=True,
            redis_timeout=180,
            raise_task_lock_exception_on_collision=False,
            lock_extend_seconds=10,
        )
        self.assertEqual(result, expected)

    def test_make_task_lock_params_with_no_host(self):
        result = make_task_lock_params(
            file_path='gs://aaa.pkl', unique_id='123', redis_host=None, redis_port=12345, redis_timeout=180, raise_task_lock_exception_on_collision=False
        )
        expected = TaskLockParams(
            redis_host=None,
            redis_port=12345,
            redis_key='aaa_123',
            should_task_lock=False,
            redis_timeout=180,
            raise_task_lock_exception_on_collision=False,
            lock_extend_seconds=10,
        )
        self.assertEqual(result, expected)

    def test_assert_when_redis_timeout_is_too_short(self):
        with self.assertRaises(AssertionError):
            make_task_lock_params(
                file_path='test_dir/test_file.pkl',
                unique_id='123abc',
                redis_host='0.0.0.0',
                redis_port=12345,
                redis_timeout=2,
            )


class TestMakeTaskLockParamsForRun(unittest.TestCase):
    def test_make_task_lock_params_for_run(self):
        class _SampleDummyTask(gokart.TaskOnKart):
            pass

        task_self = _SampleDummyTask(
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=180,
        )

        result = make_task_lock_params_for_run(task_self=task_self, lock_extend_seconds=10)
        expected = TaskLockParams(
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=180,
            redis_key='_SampleDummyTask_7e857f231830ca0fd6cf829d99f43961-run',
            should_task_lock=True,
            raise_task_lock_exception_on_collision=True,
            lock_extend_seconds=10,
        )

        self.assertEqual(result, expected)
