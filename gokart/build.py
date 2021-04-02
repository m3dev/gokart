from logging import getLogger
from typing import Optional, Any
import logging
import sys

import luigi
from gokart.task import TaskOnKart


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


def build(task: TaskOnKart, verbose: bool = False, return_value: bool = True) -> Optional[Any]:
    """
    Run gokart task for local interpreter.
    """

    # TODO: _check_env
    # TODO: load config
    # TODO: check workspace
    # TODO: fix Task Ambitious

    with HideLogger(verbose):
        luigi.build([task], local_scheduler=True)

    if return_value:
        return _get_output(task)
    return None
