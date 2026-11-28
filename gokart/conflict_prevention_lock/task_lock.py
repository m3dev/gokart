from __future__ import annotations

import os
from logging import getLogger
from typing import Any, NamedTuple

from gokart.conflict_prevention_lock.lock_config import RedisLockSettings

logger = getLogger(__name__)


class TaskLockParams(NamedTuple):
    redis_host: str | None
    redis_port: int | None
    redis_timeout: int | None
    redis_key: str
    should_task_lock: bool
    redis_db: int = 0
    redis_ssl: bool = False
    redis_use_password: bool = False


class TaskLockException(Exception):
    """Raised when the task failed to acquire the lock in the task execution. Only used internally."""


def make_task_lock_key(file_path: str, unique_id: str | None) -> str:
    basename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    return f'{basename_without_ext}_{unique_id}'


def make_task_lock_params_for_run(task_self: Any) -> TaskLockParams:
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
        redis_db=settings.db,
        redis_ssl=settings.ssl,
        redis_use_password=settings.use_password,
    )
