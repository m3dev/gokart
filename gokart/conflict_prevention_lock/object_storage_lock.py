from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Callable
from logging import getLogger
from typing import NamedTuple, Protocol, cast

from gokart.conflict_prevention_lock.lock_config import DEFAULT_SKEW_MARGIN_SECONDS

logger = getLogger(__name__)


def lock_object_name(prefix: str, key: str) -> str:
    """Name of the object that holds a lock, shared by all object stores so that a lifecycle rule on ``<prefix>/`` matches them alike."""
    prefix = prefix.strip('/')
    return f'{prefix}/{key}.lock' if prefix else f'{key}.lock'


class StoredLock(NamedTuple):
    version: str  # GCS generation / S3 ETag: changes on every write
    updated_at: float  # server-side last write time (epoch seconds)
    body: bytes


class LockLostError(Exception):
    """Raised when the lock was taken over by someone else or removed before it could be extended."""


class ObjectLockStore(Protocol):
    """Conditional writes of an object store, one object per lock. ``None`` / ``False`` means the precondition failed (412)."""

    def stat(self, key: str) -> StoredLock | None: ...

    def create_if_absent(self, key: str, body: bytes) -> str | None: ...

    def replace_if_match(self, key: str, body: bytes, version: str) -> str | None: ...

    def delete_if_match(self, key: str, version: str) -> bool: ...

    def close(self) -> None: ...


class LazyObjectLockStore:
    """Builds the store on first use, so that a missing extra or missing credentials fail the task that takes the lock, not the whole worker."""

    def __init__(self, factory: Callable[[], ObjectLockStore]) -> None:
        self._factory = factory
        self._store: ObjectLockStore | None = None

    def _get(self) -> ObjectLockStore:
        if self._store is None:
            self._store = self._factory()
        return self._store

    def stat(self, key: str) -> StoredLock | None:
        return self._get().stat(key)

    def create_if_absent(self, key: str, body: bytes) -> str | None:
        return self._get().create_if_absent(key, body)

    def replace_if_match(self, key: str, body: bytes, version: str) -> str | None:
        return self._get().replace_if_match(key, body, version)

    def delete_if_match(self, key: str, version: str) -> bool:
        return self._get().delete_if_match(key, version)

    def close(self) -> None:
        if self._store is not None:
            self._store.close()


class ObjectStorageRunLock:
    """A run lock stored as one object per lock.

    The object has no TTL on the server, so a waiter decides when the holder is gone: either the server-side
    last write time + ttl + skew_margin has passed, or it has seen the same version for ttl seconds on its own
    monotonic clock. Either way the takeover is conditioned on the observed version, so only one waiter wins.
    """

    def __init__(
        self,
        store: ObjectLockStore,
        key: str,
        ttl: int,
        skew_margin: float = DEFAULT_SKEW_MARGIN_SECONDS,
        clock: Callable[[], float] = time.time,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._key = key
        self._ttl = ttl
        self._skew_margin = skew_margin
        self._clock = clock
        self._monotonic = monotonic
        self._owner = uuid.uuid4().hex
        self._version: str | None = None
        self._observed: tuple[str, float] | None = None
        self._lost = False
        # The heartbeat calls extend() from its own thread, so release() could otherwise delete a generation that a concurrent extend() has just replaced.
        self._mutex = threading.Lock()

    def try_acquire(self) -> bool:
        # Check with a read first: GCS allows about one write per second to the same object, and waiters poll.
        current = self._store.stat(self._key)
        if current is None:
            version = self._store.create_if_absent(self._key, self._make_body())
            if version is not None:
                return self._held(version)
            current = self._store.stat(self._key)
            if current is None:
                return False
        # A write whose response was lost makes the retry fail with 412 for our own write, on create and on takeover alike.
        if self._owner_of(current) == self._owner:
            return self._held(current.version)
        if not self._is_expired(current):
            return False
        version = self._store.replace_if_match(self._key, self._make_body(), current.version)
        if version is not None:
            return self._held(version)
        current = self._store.stat(self._key)
        if current is not None and self._owner_of(current) == self._owner:
            return self._held(current.version)
        return False

    def extend(self) -> None:
        with self._mutex:
            if self._lost:
                # Already reported once; the heartbeat keeps calling until the task ends.
                return
            if self._version is None:
                raise LockLostError(f'Run lock {self._key} is not held.')
            # The nonce changes the body on every write; S3 ETags are an MD5 of the body, so waiters would otherwise see a stale lock.
            version = self._store.replace_if_match(self._key, self._make_body(), self._version)
            if version is None:
                # The write may have committed with its response lost; only we write our owner token, so adopting that version is safe.
                current = self._store.stat(self._key)
                if current is not None and self._owner_of(current) == self._owner:
                    self._version = current.version
                    return
                self._version = None
                self._lost = True
                raise LockLostError(f'Run lock {self._key} was taken over by another worker or removed.')
            self._version = version

    def release(self) -> None:
        with self._mutex:
            try:
                if self._version is None:
                    return
                version, self._version = self._version, None
                if not self._store.delete_if_match(self._key, version) and not self._delete_own_newer_generation():
                    logger.warning(f'Task RUN lock of {self._key} had already been taken over or removed before release.')
            except Exception:
                # Raising here would crash the worker loop; the lock expires after the lock timeout anyway.
                logger.warning(f'Failed to release task RUN lock of {self._key}. It expires after the lock timeout.', exc_info=True)
            finally:
                try:
                    self._store.close()
                except Exception:
                    logger.debug('Failed to close the lock store.', exc_info=True)

    def _delete_own_newer_generation(self) -> bool:
        # A heartbeat whose write committed but raised leaves the held version stale while the object is still ours.
        current = self._store.stat(self._key)
        return current is not None and self._owner_of(current) == self._owner and self._store.delete_if_match(self._key, current.version)

    def _held(self, version: str) -> bool:
        self._version = version
        self._observed = None
        return True

    def _make_body(self) -> bytes:
        return json.dumps({'owner': self._owner, 'ttl': self._ttl, 'nonce': uuid.uuid4().hex}).encode()

    @staticmethod
    def _parse(lock: StoredLock) -> dict[str, object]:
        try:
            body = json.loads(lock.body)
        except ValueError:
            return {}
        return cast(dict[str, object], body) if isinstance(body, dict) else {}

    def _owner_of(self, lock: StoredLock) -> object:
        return self._parse(lock).get('owner')

    def _is_expired(self, lock: StoredLock) -> bool:
        holder_ttl = self._parse(lock).get('ttl')
        ttl = float(holder_ttl) if isinstance(holder_ttl, int | float) else float(self._ttl)
        if lock.updated_at + ttl + self._skew_margin < self._clock():
            return True
        now = self._monotonic()
        if self._observed is None or self._observed[0] != lock.version:
            self._observed = (lock.version, now)
            return False
        return now - self._observed[1] >= ttl
