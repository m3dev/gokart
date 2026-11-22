from __future__ import annotations

import json
from typing import Any

import pytest

from gokart.conflict_prevention_lock.object_storage_lock import LockLostError, ObjectStorageRunLock, StoredLock


class FakeStore:
    """An in-memory object store with generation preconditions; the real stores are external services."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[int, bytes, float]] = {}
        self._generation = 0
        self.now = 1_000.0
        self.lose_next_create_response = False

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
        if key not in self.objects or str(self.objects[key][0]) != version:
            return None
        generation = self._next_generation()
        self.objects[key] = (generation, body, self.now)
        return str(generation)

    def delete_if_match(self, key: str, version: str) -> bool:
        if key not in self.objects or str(self.objects[key][0]) != version:
            return False
        del self.objects[key]
        return True


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


class TestExtend:
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
