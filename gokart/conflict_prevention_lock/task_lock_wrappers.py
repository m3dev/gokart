from __future__ import annotations

import functools
import warnings
from collections.abc import Callable, Generator
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar, cast

from gokart.conflict_prevention_lock.task_lock import TaskLockParams, set_lock_scheduler, set_task_lock

if TYPE_CHECKING:
    from apscheduler.schedulers.background import BackgroundScheduler  # pyright: ignore[reportMissingTypeStubs]
    from redis.lock import Lock

logger = getLogger(__name__)


P = ParamSpec('P')
R = TypeVar('R')


def _unlock(task_lock: Lock, scheduler: BackgroundScheduler) -> None:
    # Stop extending the lock before releasing it, and do both even if one raises: a scheduler left running would extend the lock forever.
    try:
        cast(Any, scheduler).shutdown()
    finally:
        task_lock.release()


def _unlock_after_error(unlock: Callable[[], None], task_lock_params: TaskLockParams, operation: str) -> None:
    try:
        unlock()
    except Exception:
        # Raising this would hide the error that is already propagating.
        logger.warning(f'Failed to release the task {operation} lock of {task_lock_params.redis_key}.', exc_info=True)
    else:
        logger.debug(f'Task {operation} lock of {task_lock_params.redis_key} released.')


@contextmanager
def _hold_lock(task_lock_params: TaskLockParams, operation: str) -> Generator[None]:
    task_lock = set_task_lock(task_lock_params=task_lock_params)
    try:
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)
    except BaseException:
        # Otherwise other workers wait for the lock timeout, or forever when no timeout is set.
        _unlock_after_error(task_lock.release, task_lock_params, operation)
        raise
    logger.debug(f'Task {operation} lock of {task_lock_params.redis_key} locked.')
    try:
        yield
    except BaseException:
        _unlock_after_error(functools.partial(_unlock, task_lock, scheduler), task_lock_params, operation)
        raise
    _unlock(task_lock, scheduler)
    logger.debug(f'Task {operation} lock of {task_lock_params.redis_key} released.')


def wrap_dump_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams, exist_check: Callable[..., bool]) -> Callable[P, R | None]:
    """Redis lock wrapper function for TargetOnKart.dump().
    When TargetOnKart.dump() is called, dump() will be wrapped with redis lock and cache existance check.
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R | None:
        with _hold_lock(task_lock_params, 'DUMP'):
            if not exist_check():
                return func(*args, **kwargs)
            return None

    return wrapper


def wrap_load_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams) -> Callable[P, R]:
    """Redis lock wrapper function for TargetOnKart.load().
    When TargetOnKart.load() is called, redis lock will be locked and released before load().
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        # Taking the lock only waits for a dump in progress; it need not be held while loading.
        with _hold_lock(task_lock_params, 'LOAD'):
            pass
        return func(*args, **kwargs)

    return wrapper


def wrap_remove_with_lock(func: Callable[P, R], task_lock_params: TaskLockParams) -> Callable[P, R]:
    """Redis lock wrapper function for TargetOnKart.remove().
    When TargetOnKart.remove() is called, remove() will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with _hold_lock(task_lock_params, 'REMOVE'):
            return func(*args, **kwargs)

    return wrapper


def wrap_run_with_lock(run_func: Callable[[], R], task_lock_params: TaskLockParams) -> Callable[[], R]:
    """Deprecated, and will be removed in gokart 2.0.0: the gokart worker takes the run lock for tasks with `should_lock_run=True`. https://github.com/m3dev/gokart/issues/524"""
    # FutureWarning, because DeprecationWarning is hidden by default unless the caller is __main__.
    warnings.warn(
        'wrap_run_with_lock is deprecated and will be removed in gokart 2.0.0. The gokart worker takes the run lock for tasks with should_lock_run=True.',
        FutureWarning,
        stacklevel=2,
    )

    @functools.wraps(run_func)
    def wrapped():
        with _hold_lock(task_lock_params, 'RUN'):
            return run_func()

    return wrapped
