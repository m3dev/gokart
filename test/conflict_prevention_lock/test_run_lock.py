import threading
from typing import Any
from unittest.mock import MagicMock, patch

import fakeredis
import luigi
import pytest
from redis.lock import Lock

import gokart
from gokart.conflict_prevention_lock.run_lock import RedisRunLock, RunLockHeartbeat, make_run_lock
from gokart.conflict_prevention_lock.task_lock import make_task_lock_params_for_run


class _LockedTask(gokart.TaskOnKart[Any]):
    task_namespace = __name__


@pytest.fixture
def server():
    server = fakeredis.FakeServer()
    with patch('gokart.conflict_prevention_lock.task_lock.redis.Redis') as redis_mock:
        redis_mock.side_effect = lambda host, port: fakeredis.FakeRedis(server=server, host=host, port=port)
        yield server


def _make_task(redis_timeout: int = 180) -> _LockedTask:
    return _LockedTask(redis_host='0.0.0.0', redis_port=12345, redis_timeout=redis_timeout, should_lock_run=True)


class TestMakeRunLock:
    def test_returns_none_when_should_lock_run_is_false(self):
        assert make_run_lock(_LockedTask(redis_host='0.0.0.0', redis_port=12345, should_lock_run=False)) is None

    def test_returns_none_for_non_gokart_task(self):
        assert make_run_lock(luigi.Task()) is None

    def test_returns_redis_lock_when_should_lock_run_is_true(self, server: fakeredis.FakeServer):
        assert isinstance(make_run_lock(_make_task()), RedisRunLock)

    def test_returns_redis_lock_by_default_when_redis_is_set(self, server: fakeredis.FakeServer):
        assert isinstance(make_run_lock(_LockedTask(redis_host='0.0.0.0', redis_port=12345)), RedisRunLock)

    def test_returns_none_by_default_when_redis_is_unset(self):
        assert make_run_lock(_LockedTask()) is None


def _patch_redis_recording_close(server: fakeredis.FakeServer, close_mocks: list[MagicMock]):
    def _make_client(host: str, port: int) -> fakeredis.FakeRedis:
        client = fakeredis.FakeRedis(server=server, host=host, port=port)
        close_mock = MagicMock(wraps=client.close)
        client.close = close_mock
        close_mocks.append(close_mock)
        return client

    return patch('gokart.conflict_prevention_lock.task_lock.redis.Redis', side_effect=_make_client)


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
