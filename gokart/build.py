import logging
from logging import getLogger
from typing import Any, Optional

import luigi

import gokart
from gokart.task import TaskOnKart


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


def _get_output(task: TaskOnKart) -> Any:
    output = task.output()
    if isinstance(output, list) or isinstance(output, tuple):
        return [t.load() for t in output]
    if isinstance(output, dict):
        return {k: t.load() for k, t in output.items()}
    return output.load()


def _reset_register(keep={'gokart', 'luigi'}):
    """reset luigi.task_register.Register._reg everytime gokart.build called to avoid TaskClassAmbigiousException"""
    luigi.task_register.Register._reg = [
        x for x in luigi.task_register.Register._reg if ((x.__module__.split('.')[0] in keep)  # keep luigi and gokart
                                                         or (issubclass(x, gokart.PandasTypeConfig)))  # PandasTypeConfig should be kept
    ]


def build(task: TaskOnKart, return_value: bool = True, reset_register: bool = True, log_level: int = logging.ERROR, **env_params) -> Optional[Any]:
    """
    Run gokart task for local interpreter.
    Sharing the most of its parameters with luigi.build (see https://luigi.readthedocs.io/en/stable/api/luigi.html?highlight=build#luigi.build)
    """
    if reset_register:
        _reset_register()
    with LoggerConfig(level=log_level):
        result = luigi.build([task], local_scheduler=True, detailed_summary=True, log_level=logging.getLevelName(log_level), **env_params)
        if result.status == luigi.LuigiStatusCode.FAILED:
            raise GokartBuildError(result.summary_text)
    return _get_output(task) if return_value else None
