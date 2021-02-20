import os
from configparser import ConfigParser
from logging import getLogger
from typing import List, NamedTuple, Set, Tuple, Union

import luigi
import pandas as pd
from luigi.cmdline_parser import CmdlineParser

import gokart

logger = getLogger(__name__)


class TaskInfo(NamedTuple):
    name: str
    unique_id: str
    path: str
    params: dict
    processing_time: str


def make_task_info(task: gokart.TaskOnKart, cache: Set[str], with_logging: bool = False) -> List[TaskInfo]:
    unique_id = task.make_unique_id()
    if with_logging:
        logger.info(f'make_task_info: {task}[{unique_id}]')
    if unique_id in cache:
        return []
    cache.add(unique_id)
    task_info = TaskInfo(
        name=task.__class__.__name__,
        unique_id=unique_id,
        path=[t.path() for t in luigi.task.flatten(task.output())],
        params=task.get_info(only_significant=True),
        processing_time=task.get_processing_time(),
    )
    result = [task_info]
    children = luigi.task.flatten(task.requires())
    for child in children:
        result += make_task_info(child, cache, with_logging=with_logging)
    return result


def make_task_info_with_unique_id(task: gokart.TaskOnKart, cache: Set[str], with_logging: bool = False) -> Tuple[List[TaskInfo], str]:
    result = make_task_info(task=task, cache=cache, with_logging=with_logging)
    unique_id = task.make_unique_id()
    return result, unique_id


def get_task_info(cmdline_args: List[str], with_logging: bool = False) -> Tuple[pd.DataFrame, str]:
    with CmdlineParser.global_instance(cmdline_args) as cp:
        result, unique_id = make_task_info_with_unique_id(cp.get_task_obj(), set(), with_logging=with_logging)
    return pd.DataFrame(result), unique_id
