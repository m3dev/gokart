from typing import Callable


def task_complete_check_wrapper(run_func: Callable, complete_check_func: Callable):

    def wrapper(*args, **kwargs):
        if not complete_check_func():
            run_func(*args, **kwargs)

    return wrapper
