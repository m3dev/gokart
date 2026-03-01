from __future__ import annotations

import functools
from collections.abc import Callable
from logging import getLogger
from typing import ParamSpec, TypeVar

from gokart.conflict_prevention_lock.task_lock import TaskLockParams, set_lock_scheduler, set_task_lock

logger = getLogger(__name__)


P = ParamSpec('P')
R = TypeVar('R')


def wrap_dump_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams, exist_check: Callable[..., bool]) -> Callable[P, R | None]:
    """Redis lock wrapper function for TargetOnKart.dump().
    When TargetOnKart.dump() is called, dump() will be wrapped with redis lock and cache existance check.
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task DUMP lock of {task_lock_params.redis_key} locked.')
            if not exist_check():
                return func(*args, **kwargs)
            return None
        finally:
            logger.debug(f'Task DUMP lock of {task_lock_params.redis_key} released.')
            task_lock.release()
            scheduler.shutdown()

    return wrapper


def wrap_load_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams) -> Callable[P, R]:
    """Redis lock wrapper function for TargetOnKart.load().
    When TargetOnKart.load() is called, redis lock will be locked and released before load().
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        logger.debug(f'Task LOAD lock of {task_lock_params.redis_key} locked.')
        task_lock.release()
        logger.debug(f'Task LOAD lock of {task_lock_params.redis_key} released.')
        scheduler.shutdown()
        result = func(*args, **kwargs)
        return result

    return wrapper


def wrap_remove_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams) -> Callable[P, R]:
    """Redis lock wrapper function for TargetOnKart.remove().
    When TargetOnKart.remove() is called, remove() will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task REMOVE lock of {task_lock_params.redis_key} locked.')
            result = func(*args, **kwargs)
            task_lock.release()
            logger.debug(f'Task REMOVE lock of {task_lock_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task REMOVE lock of {task_lock_params.redis_key} released with BaseException.')
            task_lock.release()
            scheduler.shutdown()
            raise e

    return wrapper


def wrap_run_with_lock(run_func: Callable[[], R], task_lock_params: TaskLockParams) -> Callable[[], R]:
    @functools.wraps(run_func)
    def wrapped():
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task RUN lock of {task_lock_params.redis_key} locked.')
            result = run_func()
            task_lock.release()
            logger.debug(f'Task RUN lock of {task_lock_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task RUN lock of {task_lock_params.redis_key} released with BaseException.')
            task_lock.release()
            scheduler.shutdown()
            raise e

    return wrapped
