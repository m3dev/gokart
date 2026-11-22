from __future__ import annotations

import warnings
from collections.abc import Callable, Iterator
from typing import Any

import luigi
import luigi.configuration
import pytest

import gokart
from gokart.conflict_prevention_lock import lock_config


class _Task(gokart.TaskOnKart[Any]):
    task_namespace = __name__
    n: luigi.IntParameter = luigi.IntParameter(default=0)


@pytest.fixture(autouse=True)
def _reset_warning_flag() -> Iterator[None]:
    lock_config._object_storage_warning_emitted = False
    yield
    lock_config._object_storage_warning_emitted = False


@pytest.fixture
def set_url() -> Iterator[Callable[[str], None]]:
    config = luigi.configuration.get_config()
    config.add_section('gokart_lock')

    def _set(url: str) -> None:
        config.set('gokart_lock', 'url', url)

    yield _set
    config.remove_section('gokart_lock')


@pytest.mark.parametrize('url', ['gs://my-bucket/locks', 's3://my-bucket/locks'])
def test_warns_that_no_lock_is_taken_with_an_object_storage_url(set_url: Callable[[str], None], url: str):
    set_url(url)
    with pytest.warns(UserWarning, match=r'no lock is taken when targets are dumped, loaded or removed'):
        _Task()


def test_no_warning_for_a_task_whose_dump_locks_are_taken_on_redis(set_url: Callable[[str], None]):
    set_url('gs://my-bucket/locks')
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter('always')
        _Task(redis_host='redis.internal', redis_port=6379)
    assert not [w for w in caught if 'no lock is taken' in str(w.message)]


def test_warns_only_once_per_process(set_url: Callable[[str], None]):
    set_url('gs://my-bucket/locks')
    with warnings.catch_warnings(record=True) as caught:
        # 'always' disables the once-per-location filter, so only the module flag can keep it to one warning
        warnings.simplefilter('always')
        _Task(should_lock_run=True)
        _Task(should_lock_run=True, n=1)
        _Task(n=2)
    assert len([w for w in caught if 'no lock is taken' in str(w.message)]) == 1


def test_no_warning_with_a_redis_url(set_url: Callable[[str], None]):
    set_url('redis://localhost:6379')
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        _Task()


def test_no_warning_without_a_lock_backend():
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        _Task()


def test_dump_outside_the_worker_says_that_no_lock_is_taken(set_url: Callable[[str], None], tmp_path: Any):
    set_url('gs://my-bucket/locks')
    lock_config._object_storage_warning_emitted = True

    class _LockedTask(gokart.TaskOnKart[Any]):
        task_namespace = __name__

        def output(self):
            return self.make_target('out.pkl')

    task = _LockedTask(should_lock_run=True, workspace_directory=str(tmp_path))
    with pytest.warns(UserWarning, match='No lock is taken') as caught:
        task.dump({'a': 1})
    assert not any('Falling back to the dump lock' in str(w.message) for w in caught)
