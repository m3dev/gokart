from __future__ import annotations

import functools
from collections.abc import Callable
from logging import getLogger

logger = getLogger(__name__)


def task_complete_check_wrapper(run_func: Callable, complete_check_func: Callable):
    @functools.wraps(run_func)
    def wrapper(*args, **kwargs):
        if complete_check_func():
            logger.warning(f'{run_func.__name__} is skipped because the task is already completed.')
            return
        return run_func(*args, **kwargs)

    return wrapper
