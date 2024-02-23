import functools
from logging import getLogger
from typing import Callable

from gokart.conflict_prevention_lock.task_lock import TaskLockParams, set_lock_scheduler, set_task_lock

logger = getLogger(__name__)


def wrap_run_with_lock(run_func: Callable[[], None], task_lock_params: TaskLockParams):
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
