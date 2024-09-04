import logging
from functools import partial
from logging import getLogger
from typing import Literal, Optional, TypeVar, cast, overload

import backoff
import luigi

import gokart
from gokart.conflict_prevention_lock.task_lock import TaskLockException
from gokart.target import TargetOnKart
from gokart.task import TaskOnKart

T = TypeVar('T')


class LoggerConfig:
    def __init__(self, level: int):
        self.logger = getLogger(__name__)
        self.default_level = self.logger.level
        self.level = level

    def __enter__(self):
        logging.disable(self.level - 10)  # subtract 10 to disable below self.level
        self.logger.setLevel(self.level)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        logging.disable(self.default_level - 10)  # subtract 10 to disable below self.level
        self.logger.setLevel(self.default_level)


class GokartBuildError(Exception):
    pass


class HasLockedTaskException(Exception):
    pass


class TaskLockExceptionRaisedFlag:
    def __init__(self):
        self.flag: bool = False


def _get_output(task: TaskOnKart[T]) -> T:
    output = task.output()
    # FIXME: currently, nested output is not supported
    if isinstance(output, list) or isinstance(output, tuple):
        return cast(T, [t.load() for t in output if isinstance(t, TargetOnKart)])
    if isinstance(output, dict):
        return cast(T, {k: t.load() for k, t in output.items() if isinstance(t, TargetOnKart)})
    if isinstance(output, TargetOnKart):
        return output.load()
    raise ValueError(f'output type is not supported: {type(output)}')


def _reset_register(keep={'gokart', 'luigi'}):
    """reset luigi.task_register.Register._reg everytime gokart.build called to avoid TaskClassAmbigiousException"""
    luigi.task_register.Register._reg = [
        x
        for x in luigi.task_register.Register._reg
        if (
            (x.__module__.split('.')[0] in keep)  # keep luigi and gokart
            or (issubclass(x, gokart.PandasTypeConfig))
        )  # PandasTypeConfig should be kept
    ]


@overload
def build(
    task: TaskOnKart[T],
    return_value: Literal[True] = True,
    reset_register: bool = True,
    log_level: int = logging.ERROR,
    task_lock_exception_max_tries: int = 10,
    task_lock_exception_max_wait_seconds: int = 600,
    **env_params,
) -> T: ...


@overload
def build(
    task: TaskOnKart[T],
    return_value: Literal[False],
    reset_register: bool = True,
    log_level: int = logging.ERROR,
    task_lock_exception_max_tries: int = 10,
    task_lock_exception_max_wait_seconds: int = 600,
    **env_params,
) -> None: ...


def build(
    task: TaskOnKart[T],
    return_value: bool = True,
    reset_register: bool = True,
    log_level: int = logging.ERROR,
    task_lock_exception_max_tries: int = 10,
    task_lock_exception_max_wait_seconds: int = 600,
    **env_params,
) -> Optional[T]:
    """
    Run gokart task for local interpreter.
    Sharing the most of its parameters with luigi.build (see https://luigi.readthedocs.io/en/stable/api/luigi.html?highlight=build#luigi.build)
    """
    if reset_register:
        _reset_register()
    with LoggerConfig(level=log_level):
        task_lock_exception_raised = TaskLockExceptionRaisedFlag()

        @TaskOnKart.event_handler(luigi.Event.FAILURE)
        def when_failure(task, exception):
            if isinstance(exception, TaskLockException):
                task_lock_exception_raised.flag = True

        @backoff.on_exception(
            partial(backoff.expo, max_value=task_lock_exception_max_wait_seconds), HasLockedTaskException, max_tries=task_lock_exception_max_tries
        )
        def _build_task():
            task_lock_exception_raised.flag = False
            result = luigi.build([task], local_scheduler=True, detailed_summary=True, log_level=logging.getLevelName(log_level), **env_params)
            if task_lock_exception_raised.flag:
                raise HasLockedTaskException()
            if result.status == luigi.LuigiStatusCode.FAILED:
                raise GokartBuildError(result.summary_text)
            return _get_output(task) if return_value else None

        return _build_task()
