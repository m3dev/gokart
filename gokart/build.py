from __future__ import annotations

import enum
import logging
import sys
from dataclasses import dataclass
from functools import partial
from logging import getLogger
from typing import Literal, Protocol, TypeVar, cast, overload

import backoff
import luigi
from luigi import rpc, scheduler

import gokart
import gokart.tree.task_info
from gokart import worker
from gokart.conflict_prevention_lock.task_lock import TaskLockException
from gokart.target import TargetOnKart
from gokart.task import TaskOnKart

T = TypeVar('T')

logger: logging.Logger = logging.getLogger(__name__)


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
    """Raised when ``gokart.build`` failed. This exception contains raised exceptions in the task execution."""

    def __init__(self, message, raised_exceptions: dict[str, list[Exception]]):
        super().__init__(message)
        self.raised_exceptions = raised_exceptions


class HasLockedTaskException(Exception):
    """Raised when the task failed to acquire the lock in the task execution."""


class TaskLockExceptionRaisedFlag:
    def __init__(self):
        self.flag: bool = False


class WorkerProtocol(Protocol):
    """Protocol for Worker.
    This protocol is determined by luigi.worker.Worker.
    """

    def add(self, task: TaskOnKart) -> bool: ...

    def run(self) -> bool: ...

    def __enter__(self) -> WorkerProtocol: ...

    def __exit__(self, type, value, traceback) -> Literal[False]: ...


class WorkerSchedulerFactory:
    def create_local_scheduler(self) -> scheduler.Scheduler:
        return scheduler.Scheduler(prune_on_get_work=True, record_task_history=False)

    def create_remote_scheduler(self, url) -> rpc.RemoteScheduler:
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler: scheduler.Scheduler, worker_processes: int, assistant=False) -> WorkerProtocol:
        return worker.Worker(scheduler=scheduler, worker_processes=worker_processes, assistant=assistant)


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


class TaskDumpMode(enum.Enum):
    TREE = 'tree'
    TABLE = 'table'
    NONE = 'none'


class TaskDumpOutputType(enum.Enum):
    PRINT = 'print'
    DUMP = 'dump'
    NONE = 'none'


@dataclass
class TaskDumpConfig:
    mode: TaskDumpMode = TaskDumpMode.NONE
    output_type: TaskDumpOutputType = TaskDumpOutputType.NONE


if sys.version_info < (3, 10):

    def process_task_info(task: TaskOnKart, task_dump_config: TaskDumpConfig = TaskDumpConfig()):
        logger.warning('process_task_info is not supported in Python 3.9 or lower.')
else:
    # FIXME: after dropping python3.9 support, change this import to direct implementation
    from .build_process_task_info import process_task_info


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
    task_dump_config: TaskDumpConfig = TaskDumpConfig(),
    **env_params,
) -> T | None:
    """
    Run gokart task for local interpreter.
    Sharing the most of its parameters with luigi.build (see https://luigi.readthedocs.io/en/stable/api/luigi.html?highlight=build#luigi.build)
    """
    if reset_register:
        _reset_register()
    with LoggerConfig(level=log_level):
        log_handler_before_run = logging.StreamHandler()
        logger.addHandler(log_handler_before_run)
        process_task_info(task, task_dump_config)
        logger.removeHandler(log_handler_before_run)
        log_handler_before_run.close()

        task_lock_exception_raised = TaskLockExceptionRaisedFlag()
        raised_exceptions: dict[str, list[Exception]] = dict()

        @TaskOnKart.event_handler(luigi.Event.FAILURE)
        def when_failure(task, exception):
            if isinstance(exception, TaskLockException):
                task_lock_exception_raised.flag = True
            else:
                raised_exceptions.setdefault(task.make_unique_id(), []).append(exception)

        @backoff.on_exception(
            partial(backoff.expo, max_value=task_lock_exception_max_wait_seconds), HasLockedTaskException, max_tries=task_lock_exception_max_tries
        )
        def _build_task():
            task_lock_exception_raised.flag = False
            result = luigi.build(
                [task],
                local_scheduler=True,
                detailed_summary=True,
                log_level=logging.getLevelName(log_level),
                **env_params,
            )
            if task_lock_exception_raised.flag:
                raise HasLockedTaskException()
            if result.status == luigi.LuigiStatusCode.FAILED:
                raise GokartBuildError(result.summary_text, raised_exceptions=raised_exceptions)
            return _get_output(task) if return_value else None

        return _build_task()
