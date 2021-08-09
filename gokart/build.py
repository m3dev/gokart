import logging
import sys
from logging import getLogger
from typing import Any, List, Optional

import luigi

from gokart.task import TaskOnKart
from gokart.task_info import dump_task_info_table
from gokart.utils import check_config, read_environ


class LoggerConfig:
    def __init__(self, level: int = logging.CRITICAL):
        self.logger = getLogger(__name__)
        self.default_level = self.logger.level
        self.level = level

    def __enter__(self):
        logging.disable(self.level)
        self.logger.setLevel(self.level)
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        logging.disable(self.default_level)
        self.logger.setLevel(self.default_level)


def _get_output(task: TaskOnKart) -> Any:
    output = task.output()
    if isinstance(output, list) or isinstance(output, tuple):
        return [t.load() for t in output]
    if isinstance(output, dict):
        return {k: t.load() for k, t in output.items()}
    return output.load()


def _reset_register(keep={'gokart', 'luigi'}):
    luigi.task_register.Register._reg = [x for x in luigi.task_register.Register._reg
                                         if x.__module__.split('.')[0] in keep]  # avoid TaskClassAmbigiousException


def build(task: TaskOnKart,
          return_value: bool = True,
          reset_register: bool = True,
          log_level: int = logging.CRITICAL,
          task_info_dump_path: Optional[str] = None,
          task_info_ignore_task_names: Optional[List[str]] = None) -> Optional[Any]:
    """
    Run gokart task for local interpreter.

    Args:
        return_value (bool): Whether to return the result or not.
        reset_register (bool): Whether to reset the luigi Register or not.
        log_level (int): Minimum valid log level.
        task_info_dump_path (str): Path of the output destination for TaskInfo file. To disable dumping TaskInfo file, set to `None`.
        task_info_ignore_task_names (List[str]): Task names ignored in TaskInfo.

    Returns:
        bool: The return value. True for success, False otherwise.

    """
    if reset_register:
        _reset_register()
    read_environ()
    check_config()
    with LoggerConfig(level=log_level):
        luigi.build([task], local_scheduler=True)
        dump_task_info_table(task=task, task_info_dump_path=task_info_dump_path, task_info_ignore_task_names=task_info_ignore_task_names)
    return _get_output(task) if return_value else None
