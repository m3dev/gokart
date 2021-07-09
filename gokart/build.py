import logging
import sys
from logging import getLogger
from typing import Any, Optional

import luigi

from gokart.task import TaskOnKart
from gokart.utils import check_config, read_environ


class LoggerConfig:
    def __init__(self, level: int = logging.CRITICAL):
        self.logger = getLogger(__name__)
        self.default_level = self.logger.level
        self.level = level

    def __enter__(self):
        if self.level == logging.CRITICAL:
            logging.disable(sys.maxsize)
        else:
            logging.disable(self.level)

        self.logger.setLevel(self.level)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        logging.disable(self.default_level)
        self.logger.setLevel(self.default_level)


def _get_output(task: TaskOnKart) -> Any:
    output = task.output()
    if type(output) == list:
        return [x.load() for x in output]
    return output.load()


def _reset_register(keep={'gokart', 'luigi'}):
    luigi.task_register.Register._reg = [x for x in luigi.task_register.Register._reg
                                         if x.__module__.split('.')[0] in keep]  # avoid TaskClassAmbigiousException


def build(task: TaskOnKart, return_value: bool = True, reset_register: bool = True, log_level: int = logging.CRITICAL) -> Optional[Any]:
    """
    Run gokart task for local interpreter.
    """
    if reset_register:
        _reset_register()
    read_environ()
    check_config()
    with LoggerConfig(level=log_level):
        luigi.build([task], local_scheduler=True)
    return _get_output(task) if return_value else None
