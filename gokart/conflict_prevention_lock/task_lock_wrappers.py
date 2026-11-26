from __future__ import annotations

import functools
import warnings
from collections.abc import Callable, Generator
from contextlib import contextmanager
from logging import getLogger
from typing import TYPE_CHECKING, Any, TypeVar, cast

from gokart.conflict_prevention_lock.task_lock import TaskLockParams, set_lock_scheduler, set_task_lock

if TYPE_CHECKING:
    from apscheduler.schedulers.background import BackgroundScheduler  # pyright: ignore[reportMissingTypeStubs]
    from redis.lock import Lock

logger = getLogger(__name__)


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
