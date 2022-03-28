import functools
import os
from logging import getLogger
from typing import Callable, NamedTuple

import redis
from apscheduler.schedulers.background import BackgroundScheduler

logger = getLogger(__name__)


class RedisParams(NamedTuple):
    redis_host: str
    redis_port: str
    redis_timeout: int
    redis_key: str
    should_redis_lock: bool
    redis_fail_on_collision: bool
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

    def __init__(self, host: str, port: str) -> None:
        if not hasattr(self, '_redis_client'):
            self._redis_client = redis.Redis(host=host, port=port)

    def get_redis_client(self):
        return self._redis_client


def _extend_lock(redis_lock: redis.lock.Lock, redis_timeout: int):
    redis_lock.extend(additional_time=redis_timeout, replace_ttl=True)


def _set_redis_lock(redis_params: RedisParams) -> RedisClient:
    redis_client = RedisClient(host=redis_params.redis_host, port=redis_params.redis_port).get_redis_client()
    blocking = not redis_params.redis_fail_on_collision
    redis_lock = redis.lock.Lock(redis=redis_client, name=redis_params.redis_key, timeout=redis_params.redis_timeout, thread_local=False)
    if not redis_lock.acquire(blocking=blocking):
        raise TaskLockException('Lock already taken by other task.')
    return redis_lock


def _set_lock_scheduler(redis_lock: redis.lock.Lock, redis_params: RedisParams) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    extend_lock = functools.partial(_extend_lock, redis_lock=redis_lock, redis_timeout=redis_params.redis_timeout)
    scheduler.add_job(extend_lock,
                      'interval',
                      seconds=redis_params.lock_extend_seconds,
                      max_instances=999999999,
                      misfire_grace_time=redis_params.redis_timeout,
                      coalesce=False)
    scheduler.start()
    return scheduler


def _wrap_with_lock(func, redis_params: RedisParams):
    if not redis_params.should_redis_lock:
        return func

    def wrapper(*args, **kwargs):
        redis_lock = _set_redis_lock(redis_params=redis_params)
        scheduler = _set_lock_scheduler(redis_lock=redis_lock, redis_params=redis_params)

        try:
            logger.debug(f'Task lock of {redis_params.redis_key} locked.')
            result = func(*args, **kwargs)
            redis_lock.release()
            logger.debug(f'Task lock of {redis_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task lock of {redis_params.redis_key} released with BaseException.')
            redis_lock.release()
            scheduler.shutdown()
            raise e

    return wrapper


def wrap_with_run_lock(func, redis_params: RedisParams):
    """Redis lock wrapper function for RunWithLock.
    When a fucntion is wrapped by RunWithLock, the wrapped function will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    return _wrap_with_lock(func=func, redis_params=redis_params)


def wrap_with_dump_lock(func: Callable, redis_params: RedisParams, exist_check: Callable):
    """Redis lock wrapper function for TargetOnKart.dump().
    When TargetOnKart.dump() is called, dump() will be wrapped with redis lock and cache existance check.
    https://github.com/m3dev/gokart/issues/265
    """

    if not redis_params.should_redis_lock:
        return func

    def wrapper(*args, **kwargs):
        redis_lock = _set_redis_lock(redis_params=redis_params)
        scheduler = _set_lock_scheduler(redis_lock=redis_lock, redis_params=redis_params)

        try:
            logger.debug(f'Task lock of {redis_params.redis_key} locked.')
            if not exist_check():
                func(*args, **kwargs)
        finally:
            logger.debug(f'Task lock of {redis_params.redis_key} released.')
            redis_lock.release()
            scheduler.shutdown()

    return wrapper


def wrap_with_load_lock(func, redis_params: RedisParams):
    """Redis lock wrapper function for TargetOnKart.load().
    When TargetOnKart.load() is called, redis lock will be locked and released before load().
    https://github.com/m3dev/gokart/issues/265
    """

    if not redis_params.should_redis_lock:
        return func

    def wrapper(*args, **kwargs):
        redis_lock = _set_redis_lock(redis_params=redis_params)
        scheduler = _set_lock_scheduler(redis_lock=redis_lock, redis_params=redis_params)

        logger.debug(f'Task lock of {redis_params.redis_key} locked.')
        redis_lock.release()
        logger.debug(f'Task lock of {redis_params.redis_key} released.')
        scheduler.shutdown()
        result = func(*args, **kwargs)
        return result

    return wrapper


def wrap_with_remove_lock(func, redis_params: RedisParams):
    """Redis lock wrapper function for TargetOnKart.remove().
    When TargetOnKart.remove() is called, remove() will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    return _wrap_with_lock(func=func, redis_params=redis_params)


def make_redis_key(file_path: str, unique_id: str):
    basename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    return f'{basename_without_ext}_{unique_id}'


def make_redis_params(file_path: str,
                      unique_id: str,
                      redis_host: str = None,
                      redis_port: str = None,
                      redis_timeout: int = None,
                      redis_fail_on_collision: bool = False,
                      lock_extend_seconds: int = 10):
    redis_key = make_redis_key(file_path, unique_id)
    should_redis_lock = redis_host is not None and redis_port is not None
    if redis_timeout is not None:
        assert redis_timeout > lock_extend_seconds, f'`redis_timeout` must be set greater than lock_extend_seconds:{lock_extend_seconds}, not {redis_timeout}.'
    redis_params = RedisParams(redis_host=redis_host,
                               redis_port=redis_port,
                               redis_key=redis_key,
                               should_redis_lock=should_redis_lock,
                               redis_timeout=redis_timeout,
                               redis_fail_on_collision=redis_fail_on_collision,
                               lock_extend_seconds=lock_extend_seconds)
    return redis_params
