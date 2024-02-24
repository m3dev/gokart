import functools
from logging import getLogger
from typing import Callable

from gokart.conflict_prevention_lock.task_lock import make_task_lock_params_for_run, set_lock_scheduler, set_task_lock
from gokart.task import TaskOnKart

logger = getLogger(__name__)


def wrap_run_with_lock(run_func: Callable[[], None], task_self: TaskOnKart):
    task_lock_params = make_task_lock_params_for_run(task_self=task_self)
    if not task_lock_params.should_redis_lock:
        return run_func

    @functools.wraps(run_func)
    def wrapped():
        task_lock = set_task_lock(task_lock_params=task_lock_params)
        scheduler = set_lock_scheduler(task_lock=task_lock, task_lock_params=task_lock_params)

        try:
            logger.debug(f'Task lock of {task_lock_params.redis_key} locked.')
            result = run_func()
            task_lock.release()
            logger.debug(f'Task lock of {task_lock_params.redis_key} released.')
            scheduler.shutdown()
            return result
        except BaseException as e:
            logger.debug(f'Task lock of {task_lock_params.redis_key} released with BaseException.')
            task_lock.release()
            scheduler.shutdown()
            raise e

    return wrapped
