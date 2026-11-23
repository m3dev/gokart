from __future__ import annotations

import functools
import os
from logging import getLogger
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from apscheduler.schedulers.background import BackgroundScheduler  # pyright: ignore[reportMissingTypeStubs]

from gokart.conflict_prevention_lock.lock_config import LOCK_EXTEND_SECONDS, RedisLockSettings, redis_connection_kwargs
from gokart.conflict_prevention_lock.redis_import import import_redis

if TYPE_CHECKING:
    from redis.lock import Lock

logger = getLogger(__name__)


class TaskLockParams(NamedTuple):
    redis_host: str | None
    redis_port: int | None
    redis_timeout: int | None
    redis_key: str
    should_task_lock: bool
    raise_task_lock_exception_on_collision: bool
    lock_extend_seconds: int
    redis_db: int = 0
    redis_ssl: bool = False
    redis_use_password: bool = False


class TaskLockException(Exception):
    """Raised when the task failed to acquire the lock in the task execution. Only used internally."""


class RedisClient:
    _instances: dict[Any, Any] = {}

    def __new__(cls, *args: Any, **kwargs: Any):
        key = (args, tuple(sorted(kwargs.items())))
        if cls not in cls._instances:
            cls._instances[cls] = {}
        if key not in cls._instances[cls]:
            cls._instances[cls][key] = super().__new__(cls)
        return cls._instances[cls][key]

    def __init__(self, host: str | None, port: int | None, db: int = 0, ssl: bool = False, use_password: bool = False) -> None:
        if not hasattr(self, '_redis_client'):
            self._redis_client = import_redis().Redis(**redis_connection_kwargs(host, port, db, ssl, use_password))

    def get_redis_client(self):
        return self._redis_client


def _extend_lock(task_lock: Lock, redis_timeout: int) -> None:
    task_lock.extend(additional_time=redis_timeout, replace_ttl=True)


def set_task_lock(task_lock_params: TaskLockParams) -> Lock:
    redis_client = RedisClient(
        host=task_lock_params.redis_host,
        port=task_lock_params.redis_port,
        db=task_lock_params.redis_db,
        ssl=task_lock_params.redis_ssl,
        use_password=task_lock_params.redis_use_password,
    ).get_redis_client()
    from redis.lock import Lock

    blocking = not task_lock_params.raise_task_lock_exception_on_collision
    task_lock = Lock(redis=redis_client, name=task_lock_params.redis_key, timeout=task_lock_params.redis_timeout, thread_local=False)
    if not task_lock.acquire(blocking=blocking):
        raise TaskLockException('Lock already taken by other task.')
    return task_lock


def set_lock_scheduler(task_lock: Lock, task_lock_params: TaskLockParams) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler_any = cast(Any, scheduler)
    extend_lock = functools.partial(_extend_lock, task_lock=task_lock, redis_timeout=task_lock_params.redis_timeout or 0)
    scheduler_any.add_job(
        extend_lock,
        'interval',
        seconds=task_lock_params.lock_extend_seconds,
        max_instances=999999999,
        misfire_grace_time=task_lock_params.redis_timeout,
        coalesce=False,
    )
    scheduler_any.start()
    return scheduler


def make_task_lock_key(file_path: str, unique_id: str | None) -> str:
    basename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    return f'{basename_without_ext}_{unique_id}'


def make_task_lock_params(
    file_path: str,
    unique_id: str | None,
    redis_host: str | None = None,
    redis_port: int | None = None,
    redis_timeout: int | None = None,
    raise_task_lock_exception_on_collision: bool = False,
    lock_extend_seconds: int = LOCK_EXTEND_SECONDS,
    redis_db: int = 0,
    redis_ssl: bool = False,
    redis_use_password: bool = False,
) -> TaskLockParams:
    """Build the lock parameters from explicit values only; ``[gokart_lock]`` is resolved by ``TaskOnKart``, which passes the result down.

    Reading the config here would make every caller without lock settings (in-memory targets, the temporary files of ModelTarget, ...) take locks.
    """
    redis_key = make_task_lock_key(file_path, unique_id)
    should_task_lock = redis_host is not None and redis_port is not None
    if redis_timeout is not None:
        assert redis_timeout > lock_extend_seconds, (
            f'The lock timeout (`[gokart_lock] timeout`) must be greater than lock_extend_seconds:{lock_extend_seconds}, not {redis_timeout}.'
        )
    return TaskLockParams(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_key=redis_key,
        should_task_lock=should_task_lock,
        redis_timeout=redis_timeout,
        raise_task_lock_exception_on_collision=raise_task_lock_exception_on_collision,
        lock_extend_seconds=lock_extend_seconds,
        redis_db=redis_db,
        redis_ssl=redis_ssl,
        redis_use_password=redis_use_password,
    )


def make_task_lock_params_for_run(task_self: Any, lock_extend_seconds: int = LOCK_EXTEND_SECONDS) -> TaskLockParams:
    task_path_name = os.path.join(task_self.__module__.replace('.', '/'), f'{type(task_self).__name__}')
    unique_id = task_self.make_unique_id() + '-run'
    task_lock_key = make_task_lock_key(file_path=task_path_name, unique_id=unique_id)

    settings: RedisLockSettings = task_self._lock_settings
    should_task_lock = settings.host is not None and settings.port is not None
    return TaskLockParams(
        redis_host=settings.host,
        redis_port=settings.port,
        redis_key=task_lock_key,
        should_task_lock=should_task_lock,
        redis_timeout=settings.timeout,
        raise_task_lock_exception_on_collision=True,
        lock_extend_seconds=lock_extend_seconds,
        redis_db=settings.db,
        redis_ssl=settings.ssl,
        redis_use_password=settings.use_password,
    )
