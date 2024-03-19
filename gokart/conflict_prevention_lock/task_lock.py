import functools
import os
from logging import getLogger
from typing import NamedTuple, Optional

import redis
from apscheduler.schedulers.background import BackgroundScheduler

logger = getLogger(__name__)


class TaskLockParams(NamedTuple):
    redis_host: Optional[str]
    redis_port: Optional[int]
    redis_timeout: Optional[int]
    redis_key: str
    should_task_lock: bool
    raise_task_lock_exception_on_collision: bool
    lock_extend_seconds: int


class TaskLockException(Exception):
    pass


class RedisClient:
    _instances: dict = {}

    def __new__(cls, *args, **kwargs):
        key = (args, tuple(sorted(kwargs.items())))
        if cls not in cls._instances:
            cls._instances[cls] = {}
        if key not in cls._instances[cls]:
            cls._instances[cls][key] = super(RedisClient, cls).__new__(cls)
        return cls._instances[cls][key]

    def __init__(self, host: Optional[str], port: Optional[int]) -> None:
        if not hasattr(self, '_redis_client'):
            host = host or 'localhost'
            port = port or 6379
            self._redis_client = redis.Redis(host=host, port=port)

    def get_redis_client(self):
        return self._redis_client


def _extend_lock(task_lock: redis.lock.Lock, redis_timeout: int):
    task_lock.extend(additional_time=redis_timeout, replace_ttl=True)


def set_task_lock(task_lock_params: TaskLockParams) -> redis.lock.Lock:
    redis_client = RedisClient(host=task_lock_params.redis_host, port=task_lock_params.redis_port).get_redis_client()
    blocking = not task_lock_params.raise_task_lock_exception_on_collision
    task_lock = redis.lock.Lock(redis=redis_client, name=task_lock_params.redis_key, timeout=task_lock_params.redis_timeout, thread_local=False)
    if not task_lock.acquire(blocking=blocking):
        raise TaskLockException('Lock already taken by other task.')
    return task_lock


def set_lock_scheduler(task_lock: redis.lock.Lock, task_lock_params: TaskLockParams) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    extend_lock = functools.partial(_extend_lock, task_lock=task_lock, redis_timeout=task_lock_params.redis_timeout)
    scheduler.add_job(
        extend_lock,
        'interval',
        seconds=task_lock_params.lock_extend_seconds,
        max_instances=999999999,
        misfire_grace_time=task_lock_params.redis_timeout,
        coalesce=False,
    )
    scheduler.start()
    return scheduler


def make_task_lock_key(file_path: str, unique_id: Optional[str]):
    basename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    return f'{basename_without_ext}_{unique_id}'


def make_task_lock_params(
    file_path: str,
    unique_id: Optional[str],
    redis_host: Optional[str] = None,
    redis_port: Optional[int] = None,
    redis_timeout: Optional[int] = None,
    raise_task_lock_exception_on_collision: bool = False,
    lock_extend_seconds: int = 10,
) -> TaskLockParams:
    redis_key = make_task_lock_key(file_path, unique_id)
    should_task_lock = redis_host is not None and redis_port is not None
    if redis_timeout is not None:
        assert redis_timeout > lock_extend_seconds, f'`redis_timeout` must be set greater than lock_extend_seconds:{lock_extend_seconds}, not {redis_timeout}.'
    task_lock_params = TaskLockParams(
        redis_host=redis_host,
        redis_port=redis_port,
        redis_key=redis_key,
        should_task_lock=should_task_lock,
        redis_timeout=redis_timeout,
        raise_task_lock_exception_on_collision=raise_task_lock_exception_on_collision,
        lock_extend_seconds=lock_extend_seconds,
    )
    return task_lock_params


def make_task_lock_params_for_run(task_self, lock_extend_seconds: int = 10) -> TaskLockParams:
    task_path_name = os.path.join(task_self.__module__.replace('.', '/'), f'{type(task_self).__name__}')
    unique_id = task_self.make_unique_id() + '-run'
    task_lock_key = make_task_lock_key(file_path=task_path_name, unique_id=unique_id)

    should_task_lock = task_self.redis_host is not None and task_self.redis_port is not None
    return TaskLockParams(
        redis_host=task_self.redis_host,
        redis_port=task_self.redis_port,
        redis_key=task_lock_key,
        should_task_lock=should_task_lock,
        redis_timeout=task_self.redis_timeout,
        raise_task_lock_exception_on_collision=True,
        lock_extend_seconds=lock_extend_seconds,
    )
