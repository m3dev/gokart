import typing
import warnings
from dataclasses import dataclass
from typing import Dict, List, NamedTuple, Optional, Set, Union

import luigi

from gokart.task import TaskOnKart


@dataclass
class TaskInfo:
    name: str
    unique_id: str
    output_paths: List[TaskOnKart]
    params: dict
    processing_time: str
    is_complete: str
    task_log: dict
    requires: Union['RequiredTask', List['RequiredTask'], Dict[str, 'RequiredTask']]
    children_task_infos: List['TaskInfo']

    def get_task_id(self):
        return f'{self.name}_{self.unique_id}'

    def get_task_title(self):
        return f'({self.is_complete}) {self.name}[{self.unique_id}]'

    def get_task_detail(self):
        return f'(parameter={self.params}, output={self.output_paths}, time={self.processing_time}, task_log={self.task_log})'

    def task_info_dict(self):
        return dict(name=self.name,
                    unique_id=self.unique_id,
                    output_paths=self.output_paths,
                    params=self.params,
                    processing_time=self.processing_time,
                    is_complete=self.is_complete,
                    task_log=self.task_log,
                    requires=self.requires)


class RequiredTask(NamedTuple):
    name: str
    unique_id: str


def _make_requires_info(requires):
    if isinstance(requires, TaskOnKart):
        return RequiredTask(name=requires.__class__.__name__, unique_id=requires.make_unique_id())
    elif isinstance(requires, dict):
        return {key: _make_requires_info(requires=item) for key, item in requires.items()}
    elif isinstance(requires, typing.Iterable):
        return [_make_requires_info(requires=item) for item in requires]

    raise TypeError(f'`requires` has unexpected type {type(requires)}. Must be `TaskOnKart`, `Iterarble[TaskOnKart]`, or `Dict[str, TaskOnKart]`')


def make_task_info_tree(task: TaskOnKart, ignore_task_names: Optional[List[str]] = None, cache: Optional[Dict[str, TaskInfo]] = None) -> TaskInfo:
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='Task .* without outputs has no custom complete() method')
        is_task_complete = task.complete()

    name = task.__class__.__name__
    unique_id = task.make_unique_id()
    output_paths = [t.path() for t in luigi.task.flatten(task.output())]

    cache = {} if cache is None else cache
    cache_id = f'{name}_{unique_id}_{is_task_complete}'
    if cache_id in cache:
        return cache[cache_id]

    params = task.get_info(only_significant=True)
    processing_time = task.get_processing_time()
    if type(processing_time) == float:
        processing_time = str(processing_time) + 's'
    is_complete = ('COMPLETE' if is_task_complete else 'PENDING')
    task_log = dict(task.get_task_log())
    requires = _make_requires_info(task.requires())

    children = luigi.task.flatten(task.requires())
    children_task_infos: List[TaskInfo] = []
    for child in children:
        if ignore_task_names is None or child.__class__.__name__ not in ignore_task_names:
            children_task_infos.append(make_task_info_tree(child, ignore_task_names=ignore_task_names, cache=cache))
    task_info = TaskInfo(name=name,
                         unique_id=unique_id,
                         output_paths=output_paths,
                         params=params,
                         processing_time=processing_time,
                         is_complete=is_complete,
                         task_log=task_log,
                         requires=requires,
                         children_task_infos=children_task_infos)
    cache[cache_id] = task_info
    return task_info


def make_tree_info(task_info: TaskInfo, indent: str, last: bool, details: bool, abbr: bool, visited_tasks: Set[str]):
    result = '\n' + indent
    if last:
        result += '└─-'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    result += task_info.get_task_title()

    if abbr:
        task_id = task_info.get_task_id()
        if task_id not in visited_tasks:
            visited_tasks.add(task_id)
        else:
            result += f'\n{indent}└─- ...'
            return result

    if details:
        result += task_info.get_task_detail()

    children = task_info.children_task_infos
    for index, child in enumerate(children):
        result += make_tree_info(child, indent, (index + 1) == len(children), details=details, abbr=abbr, visited_tasks=visited_tasks)
    return result


def make_tree_info_table_list(task_info: TaskInfo, visited_tasks: Set[str]):
    task_id = task_info.get_task_id()
    if task_id in visited_tasks:
        return []
    visited_tasks.add(task_id)

    result = [task_info.task_info_dict()]

    children = task_info.children_task_infos
    for child in children:
        result += make_tree_info_table_list(task_info=child, visited_tasks=visited_tasks)
    return result
