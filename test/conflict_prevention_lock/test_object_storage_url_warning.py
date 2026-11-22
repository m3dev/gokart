from __future__ import annotations

import warnings
from collections.abc import Callable, Iterator
from typing import Any

import luigi.configuration
import pytest
from luigi.task_register import Register

from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_dump_with_lock, wrap_load_with_lock, wrap_remove_with_lock
from gokart.target import make_target

_NO_LOCK = TaskLockParams(None, None, 180, 'key', False, False, 10)


@pytest.fixture
def set_url() -> Iterator[Callable[[str], None]]:
    config = luigi.configuration.get_config()
    config.add_section('gokart_lock')
    Register.clear_instance_cache()

    def _set(url: str) -> None:
        config.set('gokart_lock', 'url', url)

    yield _set
    config.remove_section('gokart_lock')
    Register.clear_instance_cache()


def _wrap_each() -> dict[str, Callable[[], Any]]:
    return {
        'dump': lambda: wrap_dump_with_lock(lambda: None, _NO_LOCK, lambda: False),
        'load': lambda: wrap_load_with_lock(lambda: None, _NO_LOCK),
        'remove': lambda: wrap_remove_with_lock(lambda: None, _NO_LOCK),
    }


@pytest.mark.parametrize('operation', ['dump', 'load', 'remove'])
@pytest.mark.parametrize('url', ['gs://my-bucket/locks'])
def test_warns_that_the_lock_is_not_taken_with_an_object_storage_url(set_url: Callable[[str], None], operation: str, url: str):
    set_url(url)
    with pytest.warns(UserWarning, match=r'dump / load / remove locks are not taken'):
        _wrap_each()[operation]()


@pytest.mark.parametrize('operation', ['dump', 'load', 'remove'])
def test_no_warning_with_a_redis_url(set_url: Callable[[str], None], operation: str):
    set_url('redis://localhost:6379')
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        _wrap_each()[operation]()


@pytest.mark.parametrize('operation', ['dump', 'load', 'remove'])
def test_no_warning_without_a_lock_backend(operation: str):
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        _wrap_each()[operation]()


def test_no_warning_when_the_worker_holds_the_run_lock(set_url: Callable[[str], None], tmp_path: Any):
    set_url('gs://my-bucket/locks')
    target = make_target(file_path=str(tmp_path / 'out.pkl'), unique_id=None)
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        # lock_at_dump=False is what the gokart worker passes for a task that holds the run lock
        target.dump({'a': 1}, lock_at_dump=False)
