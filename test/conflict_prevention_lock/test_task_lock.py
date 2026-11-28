import importlib
import unittest
from collections.abc import Callable
from typing import Any

import pytest

import gokart
from gokart.conflict_prevention_lock.task_lock import TaskLockParams, make_task_lock_key, make_task_lock_params_for_run


class TestMakeRedisKey(unittest.TestCase):
    def test_make_redis_key(self):
        result = make_task_lock_key(file_path='gs://test_ll/dir/fname.pkl', unique_id='12345')
        self.assertEqual(result, 'fname_12345')


class TestMakeTaskLockParamsForRun:
    def test_make_task_lock_params_for_run(self, lock_config: Callable[..., None]):
        class _SampleDummyTask(gokart.TaskOnKart[Any]):
            pass

        lock_config(url='redis://0.0.0.0:12345', timeout='180')
        task_self = _SampleDummyTask()

        result = make_task_lock_params_for_run(task_self=task_self)
        expected = TaskLockParams(
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=180,
            redis_key='_SampleDummyTask_7e857f231830ca0fd6cf829d99f43961-run',
            should_task_lock=True,
        )

        assert result == expected


def test_wrap_run_with_lock_is_removed() -> None:
    # Removed in gokart 2.0.0: the gokart worker takes the run lock for tasks with should_lock_run=True.
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module('gokart.conflict_prevention_lock.task_lock_wrappers')
