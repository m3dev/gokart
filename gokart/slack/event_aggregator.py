from __future__ import annotations

import contextlib
import os
from collections.abc import Callable
from logging import getLogger
from typing import Any, TypedDict, cast

import luigi

from gokart.task import TaskOnKart

logger = getLogger(__name__)


class FailureEvent(TypedDict):
    task: str
    exception: str


class EventAggregator:
    def __init__(self) -> None:
        self._success_events: list[str] = []
        self._failure_events: list[FailureEvent] = []

    def set_handlers(self):
        # WHY: registered on TaskOnKart, because _task_to_str() needs make_unique_id(), which a plain luigi.Task does not have. https://github.com/m3dev/gokart/issues/718
        for event, handler in self._handlers():
            cast(Any, TaskOnKart).event_handler(event)(handler)

    def remove_handlers(self):
        # WHY: gokart.run() calls this in a finally block, where a KeyError for an already removed handler would replace its SystemExit.
        for event, handler in self._handlers():
            with contextlib.suppress(KeyError):
                cast(Any, TaskOnKart).remove_event_handler(event, handler)

    def _handlers(self) -> list[tuple[Any, Callable[..., None]]]:
        return [(luigi.Event.SUCCESS, self._success), (luigi.Event.FAILURE, self._failure)]

    def get_summary(self) -> str:
        return f'Success: {len(self._success_events)}; Failure: {len(self._failure_events)}'

    def get_event_list(self) -> str:
        message = ''
        if len(self._failure_events) != 0:
            failure_message = os.linesep.join([f'Task: {failure["task"]}; Exception: {failure["exception"]}' for failure in self._failure_events])
            message += '---- Failure Tasks ----' + os.linesep + failure_message
        if len(self._success_events) != 0:
            success_message = os.linesep.join(self._success_events)
            message += '---- Success Tasks ----' + os.linesep + success_message
        if message == '':
            message = 'Tasks were not executed.'
        return message

    def _success(self, task: Any):
        self._success_events.append(self._task_to_str(task))

    def _failure(self, task: Any, exception: BaseException):
        failure: FailureEvent = {'task': self._task_to_str(task), 'exception': str(exception)}
        self._failure_events.append(failure)

    @staticmethod
    def _task_to_str(task: Any) -> str:
        return f'{type(task).__name__}:[{task.make_unique_id()}]'
