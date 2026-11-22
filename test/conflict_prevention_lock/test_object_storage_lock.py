from __future__ import annotations

import json
import threading
from collections.abc import Callable
from typing import Any

import pytest

from gokart.conflict_prevention_lock.object_storage_lock import LazyObjectLockStore, LockLostError, ObjectStorageRunLock, StoredLock


class FakeStore:
    """An in-memory object store with generation preconditions; the real stores are external services."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[int, bytes, float]] = {}
        self._generation = 0
        self.now = 1_000.0
        self.lose_next_create_response = False
        self.lose_next_replace_response = False
        self.delete_error: Exception | None = None
        self.raise_after_next_replace_commit: Exception | None = None
        self.before_replace: Callable[[], None] | None = None
        self.closed = False

    def _next_generation(self) -> int:
        self._generation += 1
        return self._generation

    def stat(self, key: str) -> StoredLock | None:
        if key not in self.objects:
            return None
        generation, body, updated_at = self.objects[key]
        return StoredLock(version=str(generation), updated_at=updated_at, body=body)

    def create_if_absent(self, key: str, body: bytes) -> str | None:
        if key in self.objects:
            return None
        generation = self._next_generation()
        self.objects[key] = (generation, body, self.now)
        if self.lose_next_create_response:
            self.lose_next_create_response = False
            return None
        return str(generation)

    def replace_if_match(self, key: str, body: bytes, version: str) -> str | None:
        if self.before_replace is not None:
            self.before_replace()
        if key not in self.objects or str(self.objects[key][0]) != version:
            return None
        generation = self._next_generation()
        self.objects[key] = (generation, body, self.now)
        if self.raise_after_next_replace_commit is not None:
            error, self.raise_after_next_replace_commit = self.raise_after_next_replace_commit, None
            raise error
        if self.lose_next_replace_response:
            self.lose_next_replace_response = False
            return None
        return str(generation)

    def delete_if_match(self, key: str, version: str) -> bool:
        if self.delete_error is not None:
            raise self.delete_error
        if key not in self.objects or str(self.objects[key][0]) != version:
            return False
        del self.objects[key]
        return True

    def close(self) -> None:
        self.closed = True


class Clocks:
    def __init__(self, store: FakeStore) -> None:
        self._store = store
        self.monotonic = 0.0

    def wall(self) -> float:
        return self._store.now

    def advance(self, seconds: float) -> None:
        self._store.now += seconds
        self.monotonic += seconds


@pytest.fixture
def store() -> FakeStore:
    return FakeStore()


@pytest.fixture
def clocks(store: FakeStore) -> Clocks:
    return Clocks(store)


def _lock(store: FakeStore, clocks: Clocks, ttl: int = 60, skew_margin: float = 30.0, **kwargs: Any) -> ObjectStorageRunLock:
    return ObjectStorageRunLock(store, 'task_run', ttl=ttl, skew_margin=skew_margin, clock=clocks.wall, monotonic=lambda: clocks.monotonic, **kwargs)


class TestAcquireAndRelease:
    def test_acquire_creates_the_lock_object(self, store: FakeStore, clocks: Clocks):
        assert _lock(store, clocks).try_acquire()
        body = json.loads(store.objects['task_run'][1])
        assert body['ttl'] == 60 and body['owner'] and body['nonce']

    def test_second_holder_is_rejected(self, store: FakeStore, clocks: Clocks):
        assert _lock(store, clocks).try_acquire()
        assert not _lock(store, clocks).try_acquire()

    def test_release_allows_the_next_holder(self, store: FakeStore, clocks: Clocks):
        first = _lock(store, clocks)
        assert first.try_acquire()
        first.release()
        assert 'task_run' not in store.objects
        assert _lock(store, clocks).try_acquire()

    def test_release_does_not_delete_a_lock_taken_over_by_someone_else(self, store: FakeStore, clocks: Clocks):
        first = _lock(store, clocks)
        assert first.try_acquire()
        clocks.advance(200)
        second = _lock(store, clocks)
        assert second.try_acquire()
        first.release()
        assert 'task_run' in store.objects

    def test_lost_create_response_is_recognised_as_our_own_write(self, store: FakeStore, clocks: Clocks):
        store.lose_next_create_response = True
        lock = _lock(store, clocks)
        assert lock.try_acquire()
        lock.extend()

    def test_release_deletes_our_own_object_when_a_committed_heartbeat_raised_before_the_task_finished(self, store: FakeStore, clocks: Clocks):
        lock = _lock(store, clocks)
        assert lock.try_acquire()
        store.raise_after_next_replace_commit = TimeoutError('retry deadline exceeded')
        with pytest.raises(TimeoutError):
            lock.extend()
        # the held generation is now stale, but the object is still ours
        lock.release()
        assert 'task_run' not in store.objects

    def test_release_swallows_store_errors_and_expires_by_timeout(self, store: FakeStore, clocks: Clocks, caplog: pytest.LogCaptureFixture):
        lock = _lock(store, clocks)
        lock.try_acquire()
        store.delete_error = PermissionError('403 Forbidden')
        lock.release()
        assert 'task_run' in store.objects
        assert 'Failed to release task RUN lock' in caplog.text

    def test_release_closes_the_store(self, store: FakeStore, clocks: Clocks):
        lock = _lock(store, clocks)
        lock.try_acquire()
        lock.release()
        assert store.closed

    def test_takeover_whose_response_was_lost_is_recognised_on_the_same_attempt(self, store: FakeStore, clocks: Clocks):
        _lock(store, clocks).try_acquire()
        clocks.advance(200)
        store.lose_next_replace_response = True
        lock = _lock(store, clocks)
        assert lock.try_acquire()
        lock.extend()


class TestExtend:
    def test_extend_adopts_the_new_version_when_the_response_was_lost(self, store: FakeStore, clocks: Clocks):
        lock = _lock(store, clocks)
        lock.try_acquire()
        store.lose_next_replace_response = True
        lock.extend()
        lock.extend()
        clocks.advance(200)
        # the lock is still ours, so a waiter must not be able to take it over after a normal heartbeat
        lock.extend()
        assert not _lock(store, clocks).try_acquire()

    def test_the_loss_is_raised_once_and_not_on_every_heartbeat(self, store: FakeStore, clocks: Clocks):
        first = _lock(store, clocks)
        first.try_acquire()
        clocks.advance(200)
        assert _lock(store, clocks).try_acquire()
        with pytest.raises(LockLostError):
            first.extend()
        first.extend()

    def test_release_waits_for_an_in_flight_extend_and_deletes_the_latest_generation(self, store: FakeStore, clocks: Clocks):
        lock = _lock(store, clocks)
        lock.try_acquire()
        entered, gate = threading.Event(), threading.Event()

        def _block() -> None:
            store.before_replace = None
            entered.set()
            gate.wait(5)

        store.before_replace = _block
        heartbeat = threading.Thread(target=lock.extend)
        heartbeat.start()
        assert entered.wait(5)
        releaser = threading.Thread(target=lock.release)
        releaser.start()
        releaser.join(0.2)
        assert releaser.is_alive(), 'release() must wait for the in-flight extend()'
        gate.set()
        heartbeat.join(5)
        releaser.join(5)
        assert 'task_run' not in store.objects

    def test_extend_changes_the_body_even_with_identical_content(self, store: FakeStore, clocks: Clocks):
        lock = _lock(store, clocks)
        lock.try_acquire()
        before = store.objects['task_run'][1]
        lock.extend()
        assert store.objects['task_run'][1] != before

    def test_extend_raises_when_the_lock_was_taken_over(self, store: FakeStore, clocks: Clocks):
        first = _lock(store, clocks)
        first.try_acquire()
        clocks.advance(200)
        assert _lock(store, clocks).try_acquire()
        with pytest.raises(LockLostError):
            first.extend()

    def test_extend_raises_when_the_lock_object_is_gone(self, store: FakeStore, clocks: Clocks):
        lock = _lock(store, clocks)
        lock.try_acquire()
        del store.objects['task_run']
        with pytest.raises(LockLostError):
            lock.extend()


class TestExpiry:
    def test_live_lock_is_not_taken_over(self, store: FakeStore, clocks: Clocks):
        _lock(store, clocks).try_acquire()
        waiter = _lock(store, clocks)
        clocks.advance(60 + 29)
        assert not waiter.try_acquire()

    def test_server_time_expiry_lets_a_new_waiter_take_over_at_once(self, store: FakeStore, clocks: Clocks):
        _lock(store, clocks).try_acquire()
        clocks.advance(60 + 30 + 1)
        # the waiter is created after the holder died, so it has never observed the lock before
        assert _lock(store, clocks).try_acquire()

    def test_unchanged_version_expires_on_the_monotonic_clock_even_if_server_time_looks_fresh(self, store: FakeStore, clocks: Clocks):
        _lock(store, clocks).try_acquire()
        waiter = _lock(store, clocks, skew_margin=10_000)
        assert not waiter.try_acquire()
        # the wall clock is frozen (e.g. skewed), only the monotonic clock moves
        clocks.monotonic += 61
        assert waiter.try_acquire()

    def test_heartbeat_resets_the_observation(self, store: FakeStore, clocks: Clocks):
        holder = _lock(store, clocks)
        holder.try_acquire()
        waiter = _lock(store, clocks, skew_margin=10_000)
        assert not waiter.try_acquire()
        clocks.monotonic += 40
        holder.extend()
        assert not waiter.try_acquire()
        clocks.monotonic += 40
        assert not waiter.try_acquire()

    def test_only_one_of_two_waiters_wins_the_takeover(self, store: FakeStore, clocks: Clocks):
        _lock(store, clocks).try_acquire()
        clocks.advance(200)
        results = [_lock(store, clocks).try_acquire() for _ in range(2)]
        assert results == [True, False]


class TestLazyObjectLockStore:
    def test_the_store_is_not_built_until_first_use(self, store: FakeStore):
        built: list[FakeStore] = []

        def _factory() -> FakeStore:
            built.append(store)
            return store

        lazy = LazyObjectLockStore(_factory)
        lazy.close()
        assert built == []
        assert lazy.stat('k') is None
        assert len(built) == 1

    def test_a_failing_factory_surfaces_in_try_acquire(self, clocks: Clocks):
        def _factory() -> FakeStore:
            raise ImportError('google-cloud-storage is not installed')

        lock = ObjectStorageRunLock(LazyObjectLockStore(_factory), 'task_run', ttl=60, clock=clocks.wall, monotonic=lambda: clocks.monotonic)
        with pytest.raises(ImportError):
            lock.try_acquire()
