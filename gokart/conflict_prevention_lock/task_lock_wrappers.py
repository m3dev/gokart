from __future__ import annotations

import functools
import warnings
from collections.abc import Callable
from logging import getLogger
from typing import Any, ParamSpec, TypeVar, cast

from gokart.conflict_prevention_lock.lock_config import uses_object_storage_lock
from gokart.conflict_prevention_lock.task_lock import TaskLockParams, set_lock_scheduler, set_task_lock

logger = getLogger(__name__)


P = ParamSpec('P')
R = TypeVar('R')


def _warn_if_lock_is_not_taken_on_object_storage() -> None:
    # Only the run lock lives on object storage, so a url that points there leaves dump / load / remove without a lock.
    if uses_object_storage_lock():
        warnings.warn(
            'The [gokart_lock] url points to object storage, so the dump / load / remove locks are not taken: they need Redis. '
            'Only the run lock of tasks with should_lock_run=True (taken by the gokart worker) is active.',
            UserWarning,
            stacklevel=4,
        )


def wrap_dump_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams, exist_check: Callable[..., bool]) -> Callable[P, R | None]:
    """Redis lock wrapper function for TargetOnKart.dump().
    When TargetOnKart.dump() is called, dump() will be wrapped with redis lock and cache existance check.
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        _warn_if_lock_is_not_taken_on_object_storage()
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
            cast(Any, scheduler).shutdown()

    return wrapper


def wrap_load_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams) -> Callable[P, R]:
    """Redis lock wrapper function for TargetOnKart.load().
    When TargetOnKart.load() is called, redis lock will be locked and released before load().
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        _warn_if_lock_is_not_taken_on_object_storage()
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        logger.debug(f'Task LOAD lock of {task_lock_params.redis_key} locked.')
        task_lock.release()
        logger.debug(f'Task LOAD lock of {task_lock_params.redis_key} released.')
        cast(Any, scheduler).shutdown()
        result = func(*args, **kwargs)
        return result

    return wrapper


def wrap_remove_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams) -> Callable[P, R]:
    """Redis lock wrapper function for TargetOnKart.remove().
    When TargetOnKart.remove() is called, remove() will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    if not task_lock_params.should_task_lock:
        _warn_if_lock_is_not_taken_on_object_storage()
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task REMOVE lock of {task_lock_params.redis_key} locked.')
            result = func(*args, **kwargs)
            task_lock.release()
            logger.debug(f'Task REMOVE lock of {task_lock_params.redis_key} released.')
            cast(Any, scheduler).shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task REMOVE lock of {task_lock_params.redis_key} released with BaseException.')
            task_lock.release()
            cast(Any, scheduler).shutdown()
            raise e

    return wrapper


def wrap_run_with_lock(run_func: Callable[[], R], task_lock_params: TaskLockParams) -> Callable[[], R]:
    """Deprecated: the gokart worker takes the run lock for tasks with `should_lock_run=True`. https://github.com/m3dev/gokart/issues/524"""
    warnings.warn(
        'wrap_run_with_lock is deprecated and will be removed in a future major release. '
        'The gokart worker takes the run lock for tasks with should_lock_run=True.',
        DeprecationWarning,
        stacklevel=2,
    )

    @functools.wraps(run_func)
    def wrapped():
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task RUN lock of {task_lock_params.redis_key} locked.')
            result = run_func()
            task_lock.release()
            logger.debug(f'Task RUN lock of {task_lock_params.redis_key} released.')
            cast(Any, scheduler).shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task RUN lock of {task_lock_params.redis_key} released with BaseException.')
            task_lock.release()
            cast(Any, scheduler).shutdown()
            raise e

    return wrapped
