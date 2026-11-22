import threading
from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, patch

import fakeredis
import luigi
import pytest
from luigi.task_register import Register
from redis.lock import Lock

import gokart
from gokart.conflict_prevention_lock.object_storage_lock import ObjectStorageRunLock
from gokart.conflict_prevention_lock.run_lock import RedisRunLock, RunLockHeartbeat, make_run_lock
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params_for_run


class _LockedTask(gokart.TaskOnKart[Any]):
    task_namespace = __name__


@pytest.fixture
def server():
    server = fakeredis.FakeServer()
    with patch('redis.Redis') as redis_mock:
        redis_mock.side_effect = lambda host, port: fakeredis.FakeRedis(server=server, host=host, port=port)
        yield server


def _make_task(redis_timeout: int = 180) -> _LockedTask:
    return _LockedTask(redis_host='0.0.0.0', redis_port=12345, redis_timeout=redis_timeout, should_lock_run=True)


class TestMakeRunLock:
    def test_returns_none_when_should_lock_run_is_false(self):
        assert make_run_lock(_LockedTask(redis_host='0.0.0.0', redis_port=12345)) is None

    def test_returns_none_for_non_gokart_task(self):
        assert make_run_lock(luigi.Task()) is None

    def test_returns_redis_lock_when_should_lock_run_is_true(self, server: fakeredis.FakeServer):
        assert isinstance(make_run_lock(_make_task()), RedisRunLock)


class TestRedisRunLockOptions:
    @pytest.fixture
    def recorded_kwargs(self) -> Iterator[list[dict[str, Any]]]:
        # A MagicMock client must not reach redis.lock.Lock: it registers its Lua scripts on the class with the first client it sees,
        # which would make later tests release their locks on that mock.
        recorded: list[dict[str, Any]] = []
        server = fakeredis.FakeServer()

        def _make_client(**kwargs: Any) -> fakeredis.FakeRedis:
            recorded.append(kwargs)
            return fakeredis.FakeRedis(server=server)

        with patch('redis.Redis', side_effect=_make_client):
            yield recorded

    @pytest.fixture
    def gokart_lock_url(self) -> Iterator[Any]:
        import luigi.configuration

        config = luigi.configuration.get_config()
        existed = config.has_section('gokart_lock')
        if not existed:
            config.add_section('gokart_lock')
        previous = dict(config.items('gokart_lock'))
        Register.clear_instance_cache()

        def _set(url: str) -> None:
            config.set('gokart_lock', 'url', url)

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
        Register.clear_instance_cache()

    def test_db_tls_and_password_from_the_url_reach_redis(self, recorded_kwargs: list[dict[str, Any]], gokart_lock_url: Any, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv('GOKART_REDIS_PASSWORD', 'secret-value')
        gokart_lock_url('rediss://lock-host:6380/4')
        make_run_lock(_LockedTask(should_lock_run=True))
        assert recorded_kwargs == [{'host': 'lock-host', 'port': 6380, 'db': 4, 'ssl': True, 'password': 'secret-value'}]

    def test_password_is_not_sent_to_a_task_level_host(self, recorded_kwargs: list[dict[str, Any]], gokart_lock_url: Any, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv('GOKART_REDIS_PASSWORD', 'secret-value')
        gokart_lock_url('rediss://lock-host:6380/4')
        make_run_lock(_make_task())
        assert recorded_kwargs == [{'host': '0.0.0.0', 'port': 12345}]


def _patch_redis_recording_close(server: fakeredis.FakeServer, close_mocks: list[MagicMock]):
    def _make_client(host: str, port: int) -> fakeredis.FakeRedis:
        client = fakeredis.FakeRedis(server=server, host=host, port=port)
        close_mock = MagicMock(wraps=client.close)
        client.close = close_mock
        close_mocks.append(close_mock)
        return client

    return patch('redis.Redis', side_effect=_make_client)


class TestRedisRunLock:
    def test_try_acquire_and_release(self, server: fakeredis.FakeServer):
        task = _make_task()
        key = make_task_lock_params_for_run(task_self=task).redis_key
        lock = make_run_lock(task)
        assert lock is not None

        assert lock.try_acquire()
        assert fakeredis.FakeRedis(server=server).exists(key)

        lock.release()
        assert not fakeredis.FakeRedis(server=server).exists(key)

    def test_try_acquire_returns_false_when_lock_is_held_by_another(self, server: fakeredis.FakeServer):
        task = _make_task()
        key = make_task_lock_params_for_run(task_self=task).redis_key
        other = Lock(fakeredis.FakeRedis(server=server), name=key, timeout=180, thread_local=False)
        assert other.acquire(blocking=False)

        lock = make_run_lock(task)
        assert lock is not None
        assert not lock.try_acquire()

    def test_release_does_not_raise_when_lock_is_already_expired(self, server: fakeredis.FakeServer):
        task = _make_task()
        key = make_task_lock_params_for_run(task_self=task).redis_key
        lock = make_run_lock(task)
        assert lock is not None
        assert lock.try_acquire()
        fakeredis.FakeRedis(server=server).delete(key)

        lock.release()

    @pytest.mark.parametrize('expired', [pytest.param(False, id='held'), pytest.param(True, id='already expired')])
    def test_release_closes_redis_client(self, expired: bool):
        server = fakeredis.FakeServer()
        close_mocks: list[MagicMock] = []
        task = _make_task()
        with _patch_redis_recording_close(server, close_mocks):
            lock = make_run_lock(task)
        assert lock is not None
        assert lock.try_acquire()
        if expired:
            fakeredis.FakeRedis(server=server).delete(make_task_lock_params_for_run(task_self=task).redis_key)

        lock.release()

        assert len(close_mocks) == 1
        close_mocks[0].assert_called_once()

    def test_extend_resets_ttl(self, server: fakeredis.FakeServer):
        task = _make_task(redis_timeout=100)
        key = make_task_lock_params_for_run(task_self=task).redis_key
        lock = make_run_lock(task)
        assert lock is not None
        assert lock.try_acquire()
        client = fakeredis.FakeRedis(server=server)
        client.pexpire(key, 1000)

        lock.extend()

        assert client.pttl(key) > 1000


class _ExtendProbe:
    def __init__(self) -> None:
        self.extended = threading.Event()

    def try_acquire(self) -> bool:
        return True

    def extend(self) -> None:
        self.extended.set()

    def release(self) -> None: ...


class TestRunLockHeartbeat:
    def test_extends_added_locks_periodically(self):
        heartbeat = RunLockHeartbeat(interval_seconds=0.01)
        probe = _ExtendProbe()
        try:
            heartbeat.add(probe)
            assert probe.extended.wait(timeout=10), 'lock was not extended'
        finally:
            heartbeat.stop()

    def test_does_not_extend_removed_locks(self):
        heartbeat = RunLockHeartbeat(interval_seconds=0.01)
        removed = _ExtendProbe()
        kept = _ExtendProbe()
        try:
            heartbeat.add(removed)
            heartbeat.remove(removed)
            heartbeat.add(kept)
            assert kept.extended.wait(timeout=10), 'lock was not extended'
            assert not removed.extended.is_set()
        finally:
            heartbeat.stop()


class _BlockingExtendLock:
    def __init__(self) -> None:
        self.extending = threading.Event()
        self.unblock = threading.Event()

    def try_acquire(self) -> bool:
        return True

    def extend(self) -> None:
        self.extending.set()
        self.unblock.wait(timeout=10)

    def release(self) -> None: ...


class TestRunLockHeartbeatStop:
    def test_stop_returns_with_warning_when_extend_is_stuck(self, caplog: pytest.LogCaptureFixture):
        heartbeat = RunLockHeartbeat(interval_seconds=0.01, stop_timeout_seconds=0.01)
        lock = _BlockingExtendLock()
        heartbeat.add(lock)
        assert lock.extending.wait(timeout=10), 'lock was not extended'
        try:
            with caplog.at_level('WARNING', logger='gokart.conflict_prevention_lock.run_lock'):
                heartbeat.stop()
            assert 'did not stop' in caplog.text
        finally:
            lock.unblock.set()


class TestMakeRunLockWithGcsUrl:
    @pytest.fixture
    def gs_url(self, gcs_bucket: str) -> Iterator[None]:
        import luigi.configuration

        config = luigi.configuration.get_config()
        config.add_section('gokart_lock')
        config.set('gokart_lock', 'url', f'gs://{gcs_bucket}/locks')
        Register.clear_instance_cache()
        yield
        config.remove_section('gokart_lock')
        Register.clear_instance_cache()

    def test_gs_url_returns_an_object_storage_lock(self, gs_url: None):
        lock = make_run_lock(_LockedTask(should_lock_run=True))
        assert isinstance(lock, ObjectStorageRunLock)
        assert lock.try_acquire()
        lock.release()

    def test_each_lock_gets_its_own_client(self, gs_url: None):
        from gokart.gcs_config import GCSConfig

        with patch.object(GCSConfig, 'create_storage_client', autospec=True, side_effect=GCSConfig.create_storage_client) as create:
            task = _LockedTask(should_lock_run=True)
            first = make_run_lock(task)
            second = make_run_lock(task)
            assert first is not None and second is not None
            first.try_acquire()
            second.try_acquire()
        assert create.call_count == 2
        first.release()
        second.release()

    def test_the_client_is_not_built_until_the_lock_is_used(self, gs_url: None):
        from gokart.gcs_config import GCSConfig

        with patch.object(GCSConfig, 'create_storage_client', autospec=True) as create:
            make_run_lock(_LockedTask(should_lock_run=True))
        create.assert_not_called()

    def test_explicit_task_level_redis_settings_still_win(self, gs_url: None, server: fakeredis.FakeServer):
        assert isinstance(make_run_lock(_make_task()), RedisRunLock)
