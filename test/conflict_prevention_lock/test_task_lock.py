import importlib
import subprocess
import sys
import textwrap
import unittest
from typing import Any

import pytest

import gokart
from gokart.conflict_prevention_lock.task_lock import TaskLockParams, make_task_lock_key, make_task_lock_params_for_run


class TestMakeRedisKey(unittest.TestCase):
    def test_make_redis_key(self):
        result = make_task_lock_key(file_path='gs://test_ll/dir/fname.pkl', unique_id='12345')
        self.assertEqual(result, 'fname_12345')


class TestMakeTaskLockParamsForRun(unittest.TestCase):
    def test_make_task_lock_params_for_run(self):
        class _SampleDummyTask(gokart.TaskOnKart[Any]):
            pass

        task_self = _SampleDummyTask(
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=180,
        )

        result = make_task_lock_params_for_run(task_self=task_self)
        expected = TaskLockParams(
            redis_host='0.0.0.0',
            redis_port=12345,
            redis_timeout=180,
            redis_key='_SampleDummyTask_7e857f231830ca0fd6cf829d99f43961-run',
            should_task_lock=True,
        )

        self.assertEqual(result, expected)


def test_wrap_run_with_lock_is_removed() -> None:
    # Removed in gokart 2.0.0: the gokart worker takes the run lock for tasks with should_lock_run=True.
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module('gokart.conflict_prevention_lock.task_lock_wrappers')


def test_gokart_does_not_need_apscheduler() -> None:
    code = textwrap.dedent(
        """
        import sys
        sys.modules['apscheduler'] = None
        import gokart
        import gokart.task
        import gokart.conflict_prevention_lock.run_lock
        """
    )
    subprocess.run([sys.executable, '-c', code], check=True)
