import logging
import os
import sys
from configparser import ConfigParser
from logging import getLogger
from typing import Any, Optional

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


def _read_environ():
    config = luigi.configuration.get_config()
    for key, value in os.environ.items():
        super(ConfigParser, config).set(section=None, option=key, value=value.replace('%', '%%'))


def add_config(file_path: str):
    _, ext = os.path.splitext(file_path)
    luigi.configuration.core.PARSER = ext
    assert luigi.configuration.add_config_path(file_path)


def build(task: TaskOnKart, verbose: bool = False, return_value: bool = True) -> Optional[Any]:
    """
    Run gokart task for local interpreter.
    """
    luigi.task_register.Register._reg = [x for x in luigi.task_register.Register._reg if x.__module__ != __name__]  # avoid TaskClassAmbigiousException
    _read_environ()
    with HideLogger(verbose):
        luigi.build([task], local_scheduler=True)
    return _get_output(task) if return_value else None
