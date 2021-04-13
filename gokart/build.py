import logging
import sys
from logging import getLogger
from typing import Any, Optional

import luigi

from gokart.task import TaskOnKart
from gokart.utils import check_config, read_environ


class HideLogger:
    def __init__(self, verbose: bool):
        self.verbose = verbose
        self.logger = getLogger(__name__)
        self.default_level = self.logger.level

    def __enter__(self):
        if not self.verbose:
            logging.disable(sys.maxsize)
            self.logger.setLevel(logging.CRITICAL)
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


def build(task: TaskOnKart, verbose: bool = False, return_value: bool = True, reset_register: bool = True) -> Optional[Any]:
    """
    Run gokart task for local interpreter.
    """
    if reset_register:
        _reset_register()
    read_environ()
    check_config()
    with HideLogger(verbose):
        luigi.build([task], local_scheduler=True)
    return _get_output(task) if return_value else None
