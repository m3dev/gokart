from __future__ import annotations

import warnings
from collections.abc import Iterator
from typing import Any
from unittest.mock import patch

import luigi
import luigi.configuration
import luigi.task_register
import pytest

import gokart
from gokart.conflict_prevention_lock.lock_config import parse_lock_url, redis_connection_kwargs, resolve_redis_lock_settings
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params, make_task_lock_params_for_run
from gokart.target import make_target


class _Task(gokart.TaskOnKart[Any]):
    task_namespace = __name__


def _save_text(obj: Any, path: str) -> None:
    with open(path, 'w') as f:
        f.write(str(obj))


def _load_text(path: str) -> Any:
    with open(path) as f:
        return f.read()


@pytest.fixture(autouse=True)
def _clear_task_instance_cache() -> Iterator[None]:
    # luigi returns a cached instance for equal parameters, which would skip __init__ and its checks
    luigi.task_register.Register.clear_instance_cache()
    yield
    luigi.task_register.Register.clear_instance_cache()


@pytest.fixture
def lock_config() -> Iterator[Any]:
    config = luigi.configuration.get_config()
    existed = config.has_section('gokart_lock')
    if not existed:
        config.add_section('gokart_lock')
    previous = dict(config.items('gokart_lock'))

    def _set(**values: str) -> None:
        for name, value in values.items():
            config.set('gokart_lock', name, value)

    yield _set
    # restore, so that a section from the developer's luigi.cfg survives the test
    if existed:
        for name in dict(config.items('gokart_lock')):
            if name not in previous:
                config.remove_option('gokart_lock', name)
        for name, value in previous.items():
            config.set('gokart_lock', name, value)
    else:
        config.remove_section('gokart_lock')


class TestParseLockUrl:
    def test_host_and_port(self):
        assert parse_lock_url('redis://example.com:6380') == ('example.com', 6380, 0, False)

    def test_port_defaults_to_6379(self):
        assert parse_lock_url('redis://example.com') == ('example.com', 6379, 0, False)

    def test_db_number(self):
        assert parse_lock_url('redis://example.com:6380/2') == ('example.com', 6380, 2, False)

    def test_trailing_slash_means_db_zero(self):
        assert parse_lock_url('redis://example.com/') == ('example.com', 6379, 0, False)

    def test_rediss_enables_tls(self):
        assert parse_lock_url('rediss://example.com:6380/1') == ('example.com', 6380, 1, True)

    @pytest.mark.parametrize('url', ['gs://bucket/prefix', 's3://bucket/prefix', 'http://example.com', 'localhost:6379'])
    def test_unsupported_scheme_is_rejected(self, url: str):
        with pytest.raises(ValueError, match='Unsupported lock backend URL'):
            parse_lock_url(url)

    @pytest.mark.parametrize(
        'url',
        [
            'redis://',
            'redis://host:6379/abc',
            'redis://host:6379/1/2',
            'redis://host:6379?x=1',
            'redis://host:abc',
            'redis://host:99999',
            'redis://host/\u0663',
        ],
    )
    def test_malformed_url_is_rejected(self, url: str):
        with pytest.raises(ValueError, match='Unsupported lock backend URL'):
            parse_lock_url(url)

    def test_port_zero_is_kept_rather_than_replaced_by_the_default(self):
        assert parse_lock_url('redis://example.com:0') == ('example.com', 0, 0, False)

    @pytest.mark.parametrize('url', ['redis://:pass@host:6379', 'redis://user:pass@host:6379', 'rediss://user@host'])
    def test_credentials_in_the_url_are_rejected_in_favour_of_the_environment_variable(self, url: str):
        with pytest.raises(ValueError, match='GOKART_REDIS_PASSWORD'):
            parse_lock_url(url)


class TestResolveRedisLockSettings:
    def test_nothing_configured_disables_the_lock(self):
        settings = resolve_redis_lock_settings(None, None, None)
        assert (settings.host, settings.port, settings.timeout) == (None, None, 180)

    def test_url_and_timeout_are_read_from_gokart_lock_section(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        settings = resolve_redis_lock_settings(None, None, None)
        assert (settings.host, settings.port, settings.timeout) == ('lock-host', 6380, 60)

    def test_db_tls_and_password_are_taken_from_the_url(self, lock_config: Any):
        lock_config(url='rediss://lock-host:6380/3')
        settings = resolve_redis_lock_settings(None, None, None)
        assert (settings.db, settings.ssl, settings.use_password) == (3, True, True)

    def test_db_tls_and_password_are_not_inherited_by_an_explicit_task_host(self, lock_config: Any):
        lock_config(url='rediss://lock-host:6380/3')
        with pytest.warns(FutureWarning):
            settings = resolve_redis_lock_settings('task-host', 1234, None)
        assert (settings.db, settings.ssl, settings.use_password) == (0, False, False)

    def test_explicit_task_values_take_precedence_and_warn(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        with pytest.warns(FutureWarning, match='gokart_lock'):
            settings = resolve_redis_lock_settings('task-host', 1234, 90)
        assert (settings.host, settings.port, settings.timeout) == ('task-host', 1234, 90)

    @pytest.mark.parametrize(('host', 'port'), [('task-host', None), (None, 1234)])
    def test_a_partial_task_endpoint_is_not_completed_from_the_url(self, lock_config: Any, host: str | None, port: int | None):
        lock_config(url='redis://lock-host:6380', timeout='60')
        with pytest.warns(FutureWarning):
            settings = resolve_redis_lock_settings(host, port, None)
        # both are required, as before [gokart_lock] existed, so this means "no lock"
        assert (settings.host, settings.port) == (host, port)
        assert settings.timeout == 60

    def test_no_warning_without_explicit_task_values(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380')
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            resolve_redis_lock_settings(None, None, None)


class TestTaskIntegration:
    def test_make_task_lock_params_does_not_read_the_config(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        params = make_task_lock_params(file_path='gs://aaa.pkl', unique_id='123')
        assert (params.redis_host, params.redis_port, params.should_task_lock) == (None, None, False)

    def test_task_targets_use_the_config(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380', timeout='60')
        params = _Task()._make_task_lock_params('gs://aaa.pkl', '123')
        assert (params.redis_host, params.redis_port, params.redis_timeout, params.should_task_lock) == ('lock-host', 6380, 60, True)

    def test_lock_params_carry_db_tls_and_the_password_flag_but_not_the_password(self, lock_config: Any, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv('GOKART_REDIS_PASSWORD', 'secret-value')
        lock_config(url='rediss://lock-host:6380/3')
        params = _Task()._make_task_lock_params('gs://aaa.pkl', '123')
        assert (params.redis_db, params.redis_ssl, params.redis_use_password) == (3, True, True)
        assert 'secret-value' not in repr(params)

    def test_in_memory_output_is_not_locked_when_only_a_backend_is_configured(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380')

        class _InMemoryTask(gokart.TaskOnKart[Any]):
            task_namespace = __name__

            def output(self):
                return self.make_target('feature.pkl', persist=False)

        assert not _InMemoryTask().output()._get_task_lock_params().should_task_lock

    def test_targets_made_without_lock_settings_are_not_locked(self, lock_config: Any):
        lock_config(url='redis://lock-host:6380')
        assert not make_target('gs://aaa.pkl')._get_task_lock_params().should_task_lock

    def test_the_inner_files_of_a_model_target_are_not_locked(self, lock_config: Any, tmp_path: Any):
        # The outer zip is locked, but the load_function.pkl in its private temporary directory must not take a lock on a key shared by every model.
        lock_config(url='redis://lock-host:6380')

        class _ModelTask(gokart.TaskOnKart[Any]):
            task_namespace = __name__

            def output(self):
                return self.make_model_target('model.zip', save_function=_save_text, load_function=_load_text)

        task = _ModelTask(workspace_directory=str(tmp_path), local_temporary_directory=str(tmp_path / 'tmp'))
        with (
            patch('gokart.conflict_prevention_lock.task_lock_wrappers.set_task_lock') as set_task_lock,
            patch('gokart.conflict_prevention_lock.task_lock_wrappers.set_lock_scheduler'),
        ):
            task.output().dump('model')
        assert [call.kwargs['task_lock_params'].redis_key for call in set_task_lock.call_args_list] == [f'model_{task.make_unique_id()}']

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

    def test_task_level_redis_settings_warn_exactly_once(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter('always')
            task = _Task(redis_host='localhost', redis_port=6379)
            task._make_task_lock_params('gs://aaa.pkl', '123')
            make_task_lock_params_for_run(task_self=task)
        assert [str(w.message) for w in caught if issubclass(w.category, FutureWarning) and 'redis_host' in str(w.message)] != []
        assert len([w for w in caught if issubclass(w.category, FutureWarning)]) == 1

    def test_task_level_redis_timeout_alone_warns(self):
        with pytest.warns(FutureWarning, match='redis_timeout'):
            _Task(redis_timeout=60)


class TestRedisConnectionKwargs:
    def test_only_non_default_options_are_passed(self):
        assert redis_connection_kwargs('h', 1, 0, False) == {'host': 'h', 'port': 1}

    def test_db_and_ssl_are_passed_when_set(self):
        assert redis_connection_kwargs('h', 1, 2, True) == {'host': 'h', 'port': 1, 'db': 2, 'ssl': True}

    def test_defaults_for_missing_host_and_port(self):
        assert redis_connection_kwargs(None, None, 0, False) == {'host': 'localhost', 'port': 6379}

    def test_password_is_read_from_the_fixed_environment_variable(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv('GOKART_REDIS_PASSWORD', 'secret-value')
        assert redis_connection_kwargs('h', 1, 0, False, use_password=True)['password'] == 'secret-value'

    def test_password_is_not_sent_unless_the_endpoint_comes_from_the_url(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv('GOKART_REDIS_PASSWORD', 'secret-value')
        assert 'password' not in redis_connection_kwargs('h', 1, 0, False)

    @pytest.mark.parametrize('value', [None, ''])
    def test_unset_or_empty_password_variable_means_no_password(self, monkeypatch: pytest.MonkeyPatch, value: str | None):
        monkeypatch.delenv('GOKART_REDIS_PASSWORD', raising=False)
        if value is not None:
            monkeypatch.setenv('GOKART_REDIS_PASSWORD', value)
        assert 'password' not in redis_connection_kwargs('h', 1, 0, False, use_password=True)
