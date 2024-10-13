import functools
from logging import getLogger
from typing import Callable

logger = getLogger(__name__)


def task_complete_check_wrapper(run_func: Callable[[], None], complete_check_func: Callable[[], bool]) -> Callable[[], None]:
    @functools.wraps(run_func)
    def wrapper(*args, **kwargs) -> None:
        if complete_check_func():
            logger.warning(f'{run_func.__name__} is skipped because the task is already completed.')
            return
        return run_func(*args, **kwargs)

    return wrapper
