import os
from logging import getLogger
from typing import NamedTuple, Optional

import redis
from apscheduler.schedulers.background import BackgroundScheduler

logger = getLogger(__name__)


class RedisParams(NamedTuple):
    redis_host: str = None
    redis_port: str = None
    redis_key: str = None
    should_redis_lock: bool = False


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


def with_lock(func, redis_params: RedisParams):
    if not redis_params.should_redis_lock:
        return func

    def wrapper(*args, **kwargs):
        redis_client = RedisClient(host=redis_params.redis_host, port=redis_params.redis_port).get_redis_client()
        redis_lock = redis.lock.Lock(redis=redis_client, name=redis_params.redis_key, timeout=30, blocking=True, thread_local=False)
        redis_lock.acquire()

        def extend_lock():
            redis_lock.extend(additional_time=30, replace_ttl=True)

        scheduler = BackgroundScheduler()
        scheduler.add_job(extend_lock, 'interval', seconds=10)
        scheduler.start()

        try:
            logger.debug(f'Task lock of {redis_params.redis_key} locked.')
            result = func(*args, **kwargs)
            redis_lock.release()
            logger.debug(f'Task lock of {redis_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task lock of {redis_params.redis_key} released with BaseException.')
            scheduler.shutdown()
            raise e

    return wrapper


def make_redis_key(file_path: str, unique_id: str):
    basename_without_ext = os.path.splitext(os.path.basename(file_path))[0]
    return f'{basename_without_ext}_{unique_id}'


def make_redis_params(file_path: str, unique_id: str, redis_host: str, redis_port: str):
    redis_key = make_redis_key(file_path, unique_id)
    should_redis_lock = redis_host is not None and redis_port is not None
    redis_params = RedisParams(redis_host=redis_host, redis_port=redis_port, redis_key=redis_key, should_redis_lock=should_redis_lock)
    return redis_params
