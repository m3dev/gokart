from gokart.target import TargetOnKart, make_target
from typing import List, NamedTuple, Optional, Set, Tuple
import warnings
from logging import getLogger

import luigi
import pandas as pd

from gokart.task import TaskOnKart

logger = getLogger(__name__)


class TaskInfo(NamedTuple):
    name: str
    unique_id: str
    path: List[TargetOnKart]
    params: dict
    processing_time: str
    is_complete: str
    task_log: dict


class TaskTree:
    def __init__(self, task_info: TaskInfo, children_task_trees: Tuple) -> None:
        self.task_info: TaskInfo = task_info
        self.children_task_trees: Tuple = children_task_trees

    def get_task_id(self):
        return f'{self.task_info.name}_{self.task_info.unique_id}'

    def get_task_title(self):
        return f'({self.task_info.is_complete}) {self.task_info.name}[{self.task_info.unique_id}]'

    def get_task_detail(self):
        return f'(parameter={self.task_info.params}, output={self.task_info.path}, time={self.task_info.processing_time}, task_log={self.task_info.task_log})'


def _make_task_tree(task: TaskOnKart, ignore_task_names: Optional[List[str]]) -> TaskTree:
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='Task .* without outputs has no custom complete() method')
        is_task_complete = task.complete()
    is_complete = ('COMPLETE' if is_task_complete else 'PENDING')
    name = task.__class__.__name__
    params = task.get_info(only_significant=True)
    output_paths = [t.path() for t in luigi.task.flatten(task.output())]
    processing_time = task.get_processing_time()
    if type(processing_time) == float:
        processing_time = str(processing_time) + 's'
    task_log = dict(task.get_task_log())

    task_info = TaskInfo(
        name=name,
        unique_id=task.make_unique_id(),
        path=output_paths,
        params=params,
        processing_time=processing_time,
        is_complete=is_complete,
        task_log=task_log,
    )

    children = luigi.task.flatten(task.requires())
    children_task_trees_list: List[TaskTree] = []
    for child in children:
        if ignore_task_names is None or child.__class__.__name__ not in ignore_task_names:
            children_task_trees_list.append(_make_task_tree(child, ignore_task_names=ignore_task_names))
    children_task_trees = tuple(children_task_trees_list)
    return TaskTree(task_info=task_info, children_task_trees=children_task_trees)


def _make_tree_info(task_tree: TaskTree, indent: str = '', last: bool = True, details: bool = False, abbr: bool = True, visited_tasks: Set[str] = None):
    result = '\n' + indent
    if last:
        result += '└─-'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    result += task_tree.get_task_title()

    if abbr:
        visited_tasks = visited_tasks or set()
        task_id = task_tree.get_task_id()
        if task_id not in visited_tasks:
            visited_tasks.add(task_id)
        else:
            result += f'\n{indent}└─- ...'
            return result

    if details:
        result += task_tree.get_task_detail()

    children = task_tree.children_task_trees
    for index, child_tree in enumerate(children):
        result += _make_tree_info(child_tree, indent, (index + 1) == len(children), details=details, abbr=abbr, visited_tasks=visited_tasks)
    return result


def make_tree_info(task, indent='', last=True, details=False, abbr=True, visited_tasks=None, ignore_task_names=None):
    """
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format
    """
    task_tree = _make_task_tree(task, ignore_task_names=ignore_task_names)
    result = _make_tree_info(task_tree=task_tree, indent=indent, last=last, details=details, abbr=abbr, visited_tasks=visited_tasks)
    return result


def _make_tree_info_table_list(task_tree: TaskTree, visited_tasks: Set[str]):
    task_id = task_tree.get_task_id()
    if task_id in visited_tasks:
        return []
    visited_tasks.add(task_id)

    result = [task_tree.task_info]

    children = task_tree.children_task_trees
    for child_tree in children:
        result += _make_tree_info_table_list(task_tree=child_tree, visited_tasks=visited_tasks)
    return result


def dump_task_info_table(task: TaskOnKart, task_info_dump_path: Optional[str], ignore_task_names: Optional[List[str]]):
    if task_info_dump_path is None:
        return None

    task_tree = _make_task_tree(task, ignore_task_names=ignore_task_names)

    task_info_table = pd.DataFrame(_make_tree_info_table_list(task_tree=task_tree, visited_tasks=set()))
    unique_id = task.make_unique_id()

    task_info_target = make_target(file_path=task_info_dump_path, unique_id=unique_id)
    task_info_target.dump(obj=task_info_table, lock_at_dump=False)


class tree_info(TaskOnKart):
    mode = luigi.Parameter(default='', description='This must be in ["simple", "all"].')  # type: str
    output_path = luigi.Parameter(default='tree.txt', description='Output file path.')  # type: str

    def output(self):
        return self.make_target(self.output_path, use_unique_id=False)
