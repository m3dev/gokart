import os
from logging import getLogger
from typing import List, TypedDict

import luigi

logger = getLogger(__name__)


class FailureEvent(TypedDict):
    task: str
    exception: str


class EventAggregator(object):
    def __init__(self) -> None:
        self._success_events: List[str] = []
        self._failure_events: List[FailureEvent] = []

    def set_handlers(self):
        handlers = [(luigi.Event.SUCCESS, self._success), (luigi.Event.FAILURE, self._failure)]
        for event, handler in handlers:
            luigi.Task.event_handler(event)(handler)

    def get_summary(self) -> str:
        return f'Success: {len(self._success_events)}; Failure: {len(self._failure_events)}'

    def get_event_list(self) -> str:
        message = ''
        if len(self._failure_events) != 0:
            failure_message = os.linesep.join([f"Task: {failure['task']}; Exception: {failure['exception']}" for failure in self._failure_events])
            message += '---- Failure Tasks ----' + os.linesep + failure_message
        if len(self._success_events) != 0:
            success_message = os.linesep.join(self._success_events)
            message += '---- Success Tasks ----' + os.linesep + success_message
        if message == '':
            message = 'Tasks were not executed.'
        return message

    def _success(self, task):
        self._success_events.append(self._task_to_str(task))

    def _failure(self, task, exception):
        failure = {'task': self._task_to_str(task), 'exception': str(exception)}
        self._failure_events.append(failure)

    @staticmethod
    def _task_to_str(task) -> str:
        return f'{type(task).__name__}:[{task.make_unique_id()}]'
