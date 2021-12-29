import os
from collections import defaultdict
from logging import getLogger

import luigi

logger = getLogger(__name__)


class EventAggregator(object):

    def __init__(self) -> None:
        self._event_queue = defaultdict(list)

    def set_handlers(self):
        handlers = [(luigi.Event.SUCCESS, self._success), (luigi.Event.FAILURE, self._failure)]
        for event, handler in handlers:
            luigi.Task.event_handler(event)(handler)

    def get_summary(self) -> str:
        return f"Success: {len(self._event_queue[luigi.Event.SUCCESS])}; Failure: {len(self._event_queue[luigi.Event.FAILURE])}"

    def get_event_list(self) -> str:
        message = ''
        if self._event_queue[luigi.Event.FAILURE]:
            failure_message = os.linesep.join(
                [f"Task: {failure['task']}; Exception: {failure['exception']}" for failure in self._event_queue[luigi.Event.FAILURE]])
            message += '---- Failure Tasks ----' + os.linesep + failure_message
        if self._event_queue[luigi.Event.SUCCESS]:
            success_message = os.linesep.join(self._event_queue[luigi.Event.SUCCESS])
            message += '---- Success Tasks ----' + os.linesep + success_message
        if message == '':
            message = 'Tasks were not executed.'
        return message

    def _success(self, task):
        self._event_queue[luigi.Event.SUCCESS].append(self._task_to_str(task))

    def _failure(self, task, exception):
        failure = {'task': self._task_to_str(task), 'exception': str(exception)}
        self._event_queue[luigi.Event.FAILURE].append(failure)

    @staticmethod
    def _task_to_str(task) -> str:
        return f'{type(task).__name__}:[{task.make_unique_id()}]'
