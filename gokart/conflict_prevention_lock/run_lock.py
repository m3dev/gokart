from __future__ import annotations

import threading
from logging import getLogger
from typing import Any, Protocol, cast

from gokart.conflict_prevention_lock.gcs_lock_store import GCSLockStore
from gokart.conflict_prevention_lock.lock_config import (
    gokart_lock,
    lock_url_scheme,
    parse_object_storage_lock_url,
    redis_connection_kwargs,
    uses_object_storage_lock,
)
from gokart.conflict_prevention_lock.object_storage_lock import LazyObjectLockStore, ObjectLockStore, ObjectStorageRunLock
from gokart.conflict_prevention_lock.redis_import import import_redis
from gokart.conflict_prevention_lock.s3_lock_store import S3LockStore
from gokart.conflict_prevention_lock.task_lock import TaskLockParams, make_task_lock_params_for_run

logger = getLogger(__name__)


class RunLock(Protocol):
    """A lock that prevents the same task from running on several workers at once.

    It is held by the gokart worker process while the task runs. https://github.com/m3dev/gokart/issues/524
    """

    def try_acquire(self) -> bool: ...

    def extend(self) -> None: ...

    def release(self) -> None: ...


class RedisRunLock:
    def __init__(self, task_lock_params: TaskLockParams) -> None:
        self._task_lock_params = task_lock_params
        # A dedicated client keeps the worker's heartbeat thread off the shared connection pool that forked task processes use for dump/load locks.
        redis = import_redis()
        self._client = redis.Redis(
            **redis_connection_kwargs(
                task_lock_params.redis_host,
                task_lock_params.redis_port,
                task_lock_params.redis_db,
                task_lock_params.redis_ssl,
                task_lock_params.redis_use_password,
            )
        )
        self._lock = redis.lock.Lock(redis=self._client, name=task_lock_params.redis_key, timeout=task_lock_params.redis_timeout, thread_local=False)

    def try_acquire(self) -> bool:
        return bool(self._lock.acquire(blocking=False))

    def extend(self) -> None:
        self._lock.extend(additional_time=self._task_lock_params.redis_timeout or 0, replace_ttl=True)

    def release(self) -> None:
        from redis.exceptions import LockError, RedisError

        try:
            self._lock.release()
        except LockError:
            logger.warning(f'Task RUN lock of {self._task_lock_params.redis_key} had already expired before release.')
        except RedisError:
            # Raising here would crash the worker loop; the lock expires after the lock timeout anyway.
            logger.warning(f'Failed to release task RUN lock of {self._task_lock_params.redis_key}. It expires after the lock timeout.', exc_info=True)
        finally:
            self._client.close()


def make_run_lock(task: Any) -> RunLock | None:
    if not getattr(task, 'should_lock_run', False):
        return None
    params = make_task_lock_params_for_run(task_self=task)
    config = gokart_lock()
    # Task-level redis_* settings are deprecated but still take precedence over [gokart_lock].
    if not params.should_task_lock and uses_object_storage_lock():
        return _make_object_storage_run_lock(cast(str, config.url), cast(str, lock_url_scheme(config.url)), params, config.skew_margin)
    return RedisRunLock(params)


def _make_object_storage_run_lock(url: str, scheme: str, params: TaskLockParams, skew_margin: float) -> ObjectStorageRunLock:
    bucket, prefix = parse_object_storage_lock_url(url, scheme)

    def _create_store() -> ObjectLockStore:
        # A dedicated client keeps the heartbeat thread off the client that forked task processes use.
        if scheme == 'gs':
            from gokart.gcs_config import GCSConfig

            return GCSLockStore(GCSConfig().create_storage_client(), bucket, prefix)
        from gokart.s3_config import S3Config

        return S3LockStore(S3Config().create_boto_client(), bucket, prefix)

    return ObjectStorageRunLock(LazyObjectLockStore(_create_store), params.redis_key, ttl=params.redis_timeout or 0, skew_margin=skew_margin)


class RunLockHeartbeat:
    """Extends every held run lock periodically from a single background thread."""

    def __init__(self, interval_seconds: float, stop_timeout_seconds: float = 10.0) -> None:
        self._interval_seconds = interval_seconds
        self._stop_timeout_seconds = stop_timeout_seconds
        self._locks: set[RunLock] = set()
        self._mutex = threading.Lock()
        self._stopped = threading.Event()
        self._thread: threading.Thread | None = None

    def add(self, lock: RunLock) -> None:
        with self._mutex:
            self._locks.add(lock)
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, daemon=True)
                self._thread.start()

    def remove(self, lock: RunLock) -> None:
        with self._mutex:
            self._locks.discard(lock)

    def stop(self) -> None:
        self._stopped.set()
        if self._thread is None:
            return
        # The thread is a daemon, so a heartbeat stuck in a Redis call must not block the worker from exiting.
        self._thread.join(timeout=self._stop_timeout_seconds)
        if self._thread.is_alive():
            logger.warning(f'Task RUN lock heartbeat did not stop within {self._stop_timeout_seconds} seconds.')

    def _run(self) -> None:
        while not self._stopped.wait(self._interval_seconds):
            with self._mutex:
                locks = list(self._locks)
            for lock in locks:
                try:
                    lock.extend()
                except Exception:
                    with self._mutex:
                        # The worker may release the lock between the snapshot and extend(); that is not an error.
                        still_held = lock in self._locks
                    if still_held:
                        logger.exception('Failed to extend a task RUN lock.')
