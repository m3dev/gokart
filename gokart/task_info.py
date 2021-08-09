from logging import getLogger
from typing import List, NamedTuple, Optional, Set

import luigi
import pandas as pd

from gokart.task import TaskOnKart
from gokart.target import TargetOnKart, make_target

logger = getLogger(__name__)


class TaskInfo(NamedTuple):
    name: str
    unique_id: str
    path: List[TargetOnKart]
    params: dict
    processing_time: str


def _make_task_info_list(task: TaskOnKart, cache: Set[str], ignore_task_names: Optional[List[str]]) -> List[TaskInfo]:
    unique_id = task.make_unique_id()
    if ignore_task_names is None:
        ignore_task_names = []

    logger.info(f'make_task_info_list: {task}[{unique_id}]')
    if unique_id in cache or task.__class__.__name__ in ignore_task_names:
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
        result += _make_task_info_list(child, cache, ignore_task_names=ignore_task_names)
    return result


def _make_task_info_table(task: TaskOnKart, ignore_task_names: Optional[List[str]]):
    task_info_list = _make_task_info_list(task, set(), ignore_task_names=ignore_task_names)
    return pd.DataFrame(task_info_list)


def dump_task_info_table(task: TaskOnKart, task_info_dump_path: Optional[str], task_info_ignore_task_names: Optional[List[str]]):
    if task_info_dump_path is None:
        return None

    task_info_table = _make_task_info_table(task=task, ignore_task_names=task_info_ignore_task_names)
    unique_id = task.make_unique_id()

    task_info_target = make_target(file_path=task_info_dump_path, unique_id=unique_id)
    task_info_target.dump(obj=task_info_table, lock_at_dump=False)
