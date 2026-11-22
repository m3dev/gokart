from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import Any

import luigi
import luigi.configuration
import luigi.task_register
import pytest

import gokart
from gokart.conflict_prevention_lock.lock_config import parse_lock_url, resolve_redis_lock_settings
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params, make_task_lock_params_for_run


class _Task(gokart.TaskOnKart[Any]):
    task_namespace = __name__


@pytest.fixture(autouse=True)
def _clear_task_instance_cache() -> Iterator[None]:
    # luigi returns a cached instance for equal parameters, which would skip __init__ and its checks
    luigi.task_register.Register.clear_instance_cache()
    yield
    luigi.task_register.Register.clear_instance_cache()


@pytest.fixture
def lock_config() -> Iterator[Any]:
    config = luigi.configuration.get_config()
    config.add_section('gokart_lock') if not config.has_section('gokart_lock') else None

    def _set(**values: str) -> None:
        for name, value in values.items():
            config.set('gokart_lock', name, value)

    yield _set
    config.remove_section('gokart_lock')


class TestParseLockUrl:
    def test_host_and_port(self):
        assert parse_lock_url('redis://example.com:6380') == ('example.com', 6380)

    def test_port_defaults_to_6379(self):
        assert parse_lock_url('redis://example.com') == ('example.com', 6379)

    @pytest.mark.parametrize('url', ['gs://bucket/prefix', 's3://bucket/prefix', 'http://example.com', 'localhost:6379'])
    def test_unsupported_scheme_is_rejected(self, url: str):
        with pytest.raises(ValueError, match='Unsupported lock backend URL'):
            parse_lock_url(url)

    @pytest.mark.parametrize('url', ['redis://', 'redis://host:6379/1', 'redis://user:pass@host:6379'])
    def test_unsupported_parts_are_rejected(self, url: str):
        with pytest.raises(ValueError, match='Unsupported lock backend URL'):
            parse_lock_url(url)


class TestResolveRedisLockSettings:
    def test_nothing_configured_disables_the_lock(self):
        settings = resolve_redis_lock_settings(None, None, None)
        assert (settings.host, settings.port, settings.timeout) == (None, None, 180)

    def test_url_and_timeout_are_read_from_gokart_lock_section(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        settings = resolve_redis_lock_settings(None, None, None)
        assert (settings.host, settings.port, settings.timeout) == ('lock-host', 6380, 60)

    def test_explicit_task_values_take_precedence_and_warn(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        with pytest.warns(DeprecationWarning, match='gokart_lock'):
            settings = resolve_redis_lock_settings('task-host', 1234, 90)
        assert (settings.host, settings.port, settings.timeout) == ('task-host', 1234, 90)

    def test_missing_task_values_fall_back_to_the_config(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        with pytest.warns(DeprecationWarning):
            settings = resolve_redis_lock_settings('task-host', None, None)
        assert (settings.host, settings.port, settings.timeout) == ('task-host', 6380, 60)

    def test_no_warning_without_explicit_task_values(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380')
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            resolve_redis_lock_settings(None, None, None)


class TestTaskIntegration:
    def test_dump_lock_params_use_the_config(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        params = make_task_lock_params(file_path='gs://aaa.pkl', unique_id='123')
        assert (params.redis_host, params.redis_port, params.redis_timeout, params.should_task_lock) == ('lock-host', 6380, 60, True)

    def test_run_lock_params_use_the_config(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        params = make_task_lock_params_for_run(task_self=_Task(should_lock_run=True))
        assert (params.redis_host, params.redis_port, params.redis_timeout, params.should_task_lock) == ('lock-host', 6380, 60, True)

    def test_should_lock_run_does_not_need_task_level_redis_settings(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380')
        _Task(should_lock_run=True)

    def test_should_lock_run_without_any_backend_fails(self):
        with pytest.raises(AssertionError, match='gokart_lock'):
            _Task(should_lock_run=True)

    def test_task_level_redis_settings_warn_once_at_construction(self):
        with pytest.warns(DeprecationWarning, match='redis_host'):
            _Task(redis_host='localhost', redis_port=6379)

    def test_task_level_redis_timeout_alone_warns(self):
        with pytest.warns(DeprecationWarning, match='redis_timeout'):
            _Task(redis_timeout=60)
