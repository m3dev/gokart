from __future__ import annotations

import ctypes
import enum
import io
import logging
import multiprocessing
from collections.abc import Callable
from dataclasses import dataclass
from logging import getLogger
from types import TracebackType
from typing import Any, Literal, Protocol, Self, TypeVar, cast, overload

import luigi
from luigi import LuigiStatusCode, rpc, scheduler, task_register
from luigi.execution_summary import LuigiRunResult
from luigi.task import Task
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_random_exponential

import gokart
import gokart.tree.task_info
from gokart import worker
from gokart.conflict_prevention_lock.task_lock import TaskLockException
from gokart.in_memory.repository import in_memory_scope
from gokart.target import TargetOnKart
from gokart.task import TaskOnKart

T = TypeVar('T')

logger: logging.Logger = logging.getLogger(__name__)


class LoggerConfig:
    def __init__(self, level: int):
        self.logger = getLogger(__name__)
        self.default_level = self.logger.level
        self.default_disable_level = logging.root.manager.disable
        self.level = level

    def __enter__(self):
        logging.disable(self.level - 10)  # subtract 10 to disable below self.level
        self.logger.setLevel(self.level)
        return self

    def __exit__(self, exception_type: type[BaseException] | None, exception_value: BaseException | None, traceback: TracebackType | None):
        logging.disable(self.default_disable_level)
        self.logger.setLevel(self.default_level)


class GokartBuildError(Exception):
    """Raised when ``gokart.build`` failed. This exception contains raised exceptions in the task execution.

    ``raised_exceptions`` holds the exceptions of failed task runs. ``status`` is the ``luigi.LuigiStatusCode`` of the build
    and is the authoritative signal: for ``MISSING_EXT`` / ``NOT_RUN`` / ``SCHEDULING_FAILED`` no task run failed, so ``raised_exceptions`` has nothing for them.
    """

    def __init__(self, message: str, raised_exceptions: dict[str, list[Exception]], status: LuigiStatusCode | None = None) -> None:
        super().__init__(message)
        self.raised_exceptions = raised_exceptions
        self.status = status

    def __reduce__(self) -> tuple[Any, ...]:
        # WHY: Exception.__reduce__ rebuilds the error as cls(*self.args), which lacks the required raised_exceptions.
        # Then the error can't be sent back from a multiprocessing worker. __dict__ keeps the other attributes.
        return (self.__class__, (*self.args, self.raised_exceptions), self.__dict__)


class HasLockedTaskException(Exception):
    """Raised when the task failed to acquire the lock in the task execution."""


class TaskLockExceptionRaisedFlag:
    def __init__(self):
        # With workers > 1, tasks run in forked processes and the FAILURE event is triggered there.
        # Keep the flag in shared memory so that the parent process can see it.
        self._flag = multiprocessing.get_context('fork').RawValue(ctypes.c_bool, False)

    @property
    def flag(self) -> bool:
        return bool(self._flag.value)

    @flag.setter
    def flag(self, value: bool) -> None:
        self._flag.value = value


class WorkerProtocol(Protocol):
    """Protocol for Worker.
    This protocol is determined by luigi.worker.Worker.
    """

    def add(self, task: TaskOnKart[Any]) -> bool: ...

    def run(self) -> bool: ...

    def __enter__(self) -> Self: ...

    def __exit__(self, type: Any, value: Any, traceback: Any) -> Literal[False]: ...


class WorkerSchedulerFactory:
    def __init__(self, on_child_process_failure: Callable[[Task, BaseException], None] | None = None) -> None:
        self._on_child_process_failure = on_child_process_failure

    def create_local_scheduler(self) -> scheduler.Scheduler:
        return scheduler.Scheduler(prune_on_get_work=True, record_task_history=False)

    def create_remote_scheduler(self, url: str) -> rpc.RemoteScheduler:
        return rpc.RemoteScheduler(url)

    def create_worker(self, scheduler: scheduler.Scheduler, worker_processes: int, assistant: bool = False) -> WorkerProtocol:
        return worker.Worker(
            scheduler=scheduler, worker_processes=worker_processes, assistant=assistant, on_child_process_failure=self._on_child_process_failure
        )


def _get_output(task: TaskOnKart[T]) -> T:
    output = task.output()
    # FIXME: currently, nested output is not supported
    if isinstance(output, (list, tuple)):
        return cast(T, [t.load() for t in output if isinstance(t, TargetOnKart)])
    if isinstance(output, dict):
        return cast(T, {k: t.load() for k, t in output.items() if isinstance(t, TargetOnKart)})
    if isinstance(output, TargetOnKart):
        return cast(T, output.load())
    raise ValueError(f'output type is not supported: {type(output)}')


def _reset_register(keep: set[str] = {'gokart', 'luigi'}):
    """reset task_register.Register._reg everytime gokart.build called to avoid TaskClassAmbigiousException"""
    task_register.Register._reg = [  # pyright: ignore[reportPrivateUsage]
        x
        for x in task_register.Register._reg  # pyright: ignore[reportPrivateUsage]
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


def process_task_info(task: TaskOnKart[Any], task_dump_config: TaskDumpConfig = TaskDumpConfig()) -> None:
    match task_dump_config:
        case TaskDumpConfig(mode=TaskDumpMode.NONE, output_type=TaskDumpOutputType.NONE):
            pass
        case TaskDumpConfig(mode=TaskDumpMode.TREE, output_type=TaskDumpOutputType.PRINT):
            tree = gokart.make_tree_info(task)
            logger.info(tree)
        case TaskDumpConfig(mode=TaskDumpMode.TABLE, output_type=TaskDumpOutputType.PRINT):
            table = gokart.tree.task_info.make_task_info_as_table(task)
            output = io.StringIO()
            table.to_csv(output, index=False, sep='\t')
            output.seek(0)
            logger.info(output.read())
        case TaskDumpConfig(mode=TaskDumpMode.TREE, output_type=TaskDumpOutputType.DUMP):
            tree = gokart.make_tree_info(task)
            gokart.TaskOnKart().make_target(f'log/task_info/{type(task).__name__}.txt').dump(tree)
        case TaskDumpConfig(mode=TaskDumpMode.TABLE, output_type=TaskDumpOutputType.DUMP):
            table = gokart.tree.task_info.make_task_info_as_table(task)
            gokart.TaskOnKart().make_target(f'log/task_info/{type(task).__name__}.pkl').dump(table)
        case _:
            raise ValueError(f'Unsupported TaskDumpConfig: {task_dump_config}')


@overload
def build(
    task: TaskOnKart[T],
    return_value: Literal[True] = True,
    reset_register: bool = True,
    log_level: int = logging.ERROR,
    task_lock_exception_max_tries: int = 10,
    task_lock_exception_max_wait_seconds: int = 600,
    **env_params: Any,
) -> T: ...


@overload
def build(
    task: TaskOnKart[T],
    return_value: Literal[False],
    reset_register: bool = True,
    log_level: int = logging.ERROR,
    task_lock_exception_max_tries: int = 10,
    task_lock_exception_max_wait_seconds: int = 600,
    **env_params: Any,
) -> None: ...


def build(
    task: TaskOnKart[T],
    return_value: bool = True,
    reset_register: bool = True,
    log_level: int = logging.ERROR,
    task_lock_exception_max_tries: int = 10,
    task_lock_exception_max_wait_seconds: int = 600,
    task_dump_config: TaskDumpConfig = TaskDumpConfig(),
    **env_params: Any,
) -> T | None:
    """
    Run gokart task for local interpreter.
    Sharing the most of its parameters with luigi.build (see https://luigi.readthedocs.io/en/stable/api/luigi.html?highlight=build#luigi.build)
    """
    if reset_register:
        _reset_register()
    # WHY: in-memory outputs live only for this build, so a second build never sees the previous build's results.
    with in_memory_scope(), LoggerConfig(level=log_level):
        log_handler_before_run = logging.StreamHandler()
        logger.addHandler(log_handler_before_run)
        process_task_info(task, task_dump_config)
        logger.removeHandler(log_handler_before_run)
        log_handler_before_run.close()

        task_lock_exception_raised = TaskLockExceptionRaisedFlag()
        raised_exceptions: dict[str, list[Exception]] = {}

        @cast(Any, TaskOnKart).event_handler(luigi.Event.FAILURE)
        def when_failure(task: Any, exception: BaseException):
            if isinstance(exception, TaskLockException):
                task_lock_exception_raised.flag = True
            else:
                # WHY: a task run can fail with a BaseException such as SystemExit. It is kept as is, but the public type of
                # GokartBuildError.raised_exceptions stays list[Exception], since widening it would break typed callers.
                raised_exceptions.setdefault(task.make_unique_id(), []).append(cast(Exception, exception))

        def when_child_process_failure(task: Any, exception: BaseException):
            # WHY: the worker calls this for every task, but the FAILURE event handler above fires only for TaskOnKart.
            if isinstance(task, TaskOnKart):
                when_failure(task, exception)

        @retry(
            retry=retry_if_exception_type(HasLockedTaskException),
            stop=stop_after_attempt(task_lock_exception_max_tries),
            wait=wait_random_exponential(multiplier=0.5, max=task_lock_exception_max_wait_seconds),
            reraise=True,
        )
        def _build_task():
            task_lock_exception_raised.flag = False
            result = cast(Any, luigi).build(
                [task],
                worker_scheduler_factory=WorkerSchedulerFactory(on_child_process_failure=when_child_process_failure),
                local_scheduler=True,
                detailed_summary=True,
                log_level=logging.getLevelName(log_level),
                **env_params,
            )
            if task_lock_exception_raised.flag:
                raise HasLockedTaskException()
            result = cast(LuigiRunResult, result)
            # WHY: MISSING_EXT / NOT_RUN mean the requested task did not run. Note that luigi reports SUCCESS_WITH_RETRY even if
            # other problems remain, as long as every failed task was retried successfully (see #690).
            if result.status not in (LuigiStatusCode.SUCCESS, LuigiStatusCode.SUCCESS_WITH_RETRY):
                raise GokartBuildError(result.summary_text, raised_exceptions=raised_exceptions, status=result.status)
            return _get_output(task) if return_value else None

        try:
            return _build_task()
        finally:
            cast(Any, TaskOnKart).remove_event_handler(luigi.Event.FAILURE, when_failure)
