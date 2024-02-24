from logging import getLogger
from typing import Callable

from gokart.conflict_prevention_lock.task_lock import TaskLockParams, set_lock_scheduler, set_task_lock

logger = getLogger(__name__)


def _wrap_with_lock(func, task_lock_params: TaskLockParams):
    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args, **kwargs):
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task lock of {task_lock_params.redis_key} locked.')
            result = func(*args, **kwargs)
            task_lock.release()
            logger.debug(f'Task lock of {task_lock_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task lock of {task_lock_params.redis_key} released with BaseException.')
            task_lock.release()
            scheduler.shutdown()
            raise e

    return wrapper


def wrap_with_run_lock(func: Callable, task_lock_params: TaskLockParams):
    """Redis lock wrapper function for RunWithLock.
    When a fucntion is wrapped by RunWithLock, the wrapped function will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    return _wrap_with_lock(func=func, task_lock_params=task_lock_params)


def wrap_with_dump_lock(func: Callable, task_lock_params: TaskLockParams, exist_check: Callable):
    """Redis lock wrapper function for TargetOnKart.dump().
    When TargetOnKart.dump() is called, dump() will be wrapped with redis lock and cache existance check.
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args, **kwargs):
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task lock of {task_lock_params.redis_key} locked.')
            if not exist_check():
                func(*args, **kwargs)
        finally:
            logger.debug(f'Task lock of {task_lock_params.redis_key} released.')
            task_lock.release()
            scheduler.shutdown()

    return wrapper


def wrap_with_load_lock(func, task_lock_params: TaskLockParams):
    """Redis lock wrapper function for TargetOnKart.load().
    When TargetOnKart.load() is called, redis lock will be locked and released before load().
    https://github.com/m3dev/gokart/issues/265
    """

    if not task_lock_params.should_task_lock:
        return func

    def wrapper(*args, **kwargs):
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        logger.debug(f'Task lock of {task_lock_params.redis_key} locked.')
        task_lock.release()
        logger.debug(f'Task lock of {task_lock_params.redis_key} released.')
        scheduler.shutdown()
        result = func(*args, **kwargs)
        return result

    return wrapper


def wrap_with_remove_lock(func, task_lock_params: TaskLockParams):
    """Redis lock wrapper function for TargetOnKart.remove().
    When TargetOnKart.remove() is called, remove() will be simply wrapped with redis lock.
    https://github.com/m3dev/gokart/issues/265
    """
    return _wrap_with_lock(func=func, task_lock_params=task_lock_params)
