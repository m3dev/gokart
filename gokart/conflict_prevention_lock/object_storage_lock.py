from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable
from logging import getLogger
from typing import NamedTuple, Protocol, cast

logger = getLogger(__name__)

DEFAULT_SKEW_MARGIN_SECONDS = 30.0


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
            # A create whose response was lost makes the retry fail with 412 for our own write.
            if self._owner_of(current) == self._owner:
                return self._held(current.version)
        if not self._is_expired(current):
            return False
        version = self._store.replace_if_match(self._key, self._make_body(), current.version)
        return version is not None and self._held(version)

    def extend(self) -> None:
        if self._version is None:
            raise LockLostError(f'Run lock {self._key} is not held.')
        # The nonce changes the body on every write; S3 ETags are an MD5 of the body, so waiters would otherwise see a stale lock.
        version = self._store.replace_if_match(self._key, self._make_body(), self._version)
        if version is None:
            self._version = None
            raise LockLostError(f'Run lock {self._key} was taken over by another worker or removed.')
        self._version = version

    def release(self) -> None:
        if self._version is None:
            return
        version, self._version = self._version, None
        if not self._store.delete_if_match(self._key, version):
            logger.warning(f'Task RUN lock of {self._key} had already been taken over or removed before release.')

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
