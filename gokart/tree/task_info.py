import os
from typing import List, Optional

import pandas as pd

from gokart.target import make_target
from gokart.task import TaskOnKart
from gokart.tree.task_info_formatter import make_task_info_tree, make_tree_info, make_tree_info_table_list


def make_task_info_as_tree_str(task: TaskOnKart, details: bool = False, abbr: bool = True, ignore_task_names: Optional[List[str]] = None):
    """
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format

    Parameters
    ----------
    - task: TaskOnKart
        Root task.
    - details: bool
        Whether or not to output details.
    - abbr: bool
        Whether or not to simplify tasks information that has already appeared.
    - ignore_task_names: Optional[List[str]]
        List of task names to ignore.
    Returns
    -------
    - tree_info : str
        Formatted task dependency tree.
    """
    task_info = make_task_info_tree(task, ignore_task_names=ignore_task_names)
    result = make_tree_info(task_info=task_info, indent='', last=True, details=details, abbr=abbr, visited_tasks=set())
    return result


def make_task_info_as_table(task: TaskOnKart, ignore_task_names: Optional[List[str]] = None):
    """Return a table containing information about dependent tasks.

    Parameters
    ----------
    - task: TaskOnKart
        Root task.
    - ignore_task_names: Optional[List[str]]
        List of task names to ignore.
    Returns
    -------
    - task_info_table : pandas.DataFrame
        Formatted task dependency table.
    """

    task_info = make_task_info_tree(task, ignore_task_names=ignore_task_names)
    task_info_table = pd.DataFrame(make_tree_info_table_list(task_info=task_info, visited_tasks=set()))

    return task_info_table


def dump_task_info_table(task: TaskOnKart, task_info_dump_path: str, ignore_task_names: Optional[List[str]] = None):
    """Dump a table containing information about dependent tasks.

    Parameters
    ----------
    - task: TaskOnKart
        Root task.
    - task_info_dump_path: str
        Output target file path. Path destination can be `local`, `S3`, or `GCS`.
        File extension can be any type that gokart file processor accepts, including `csv`, `pickle`, or `txt`.
        See `TaskOnKart.make_target module <https://gokart.readthedocs.io/en/latest/task_on_kart.html#taskonkart-make-target>` for details.
    - ignore_task_names: Optional[List[str]]
        List of task names to ignore.
    Returns
    -------
    None
    """
    task_info_table = make_task_info_as_table(task=task, ignore_task_names=ignore_task_names)

    unique_id = task.make_unique_id()

    task_info_target = make_target(file_path=task_info_dump_path, unique_id=unique_id)
    task_info_target.dump(obj=task_info_table, lock_at_dump=False)


def dump_task_info_tree(task: TaskOnKart, task_info_dump_path: str, ignore_task_names: Optional[List[str]] = None, use_unique_id: bool = True):
    """Dump the task info tree object (TaskInfo) to a pickle file.

    Parameters
    ----------
    - task: TaskOnKart
        Root task.
    - task_info_dump_path: str
        Output target file path. Path destination can be `local`, `S3`, or `GCS`.
        File extension must be '.pkl'.
    - ignore_task_names: Optional[List[str]]
        List of task names to ignore.
    - use_unique_id: bool = True
        Whether to use unique id to dump target file. Default is True.
    Returns
    -------
    None
    """
    extension = os.path.splitext(task_info_dump_path)[1]
    assert extension == '.pkl', f'File extention must be `.pkl`, not `{extension}`.'

    task_info_tree = make_task_info_tree(task, ignore_task_names=ignore_task_names)

    unique_id = task.make_unique_id() if use_unique_id else None

    task_info_target = make_target(file_path=task_info_dump_path, unique_id=unique_id)
    task_info_target.dump(obj=task_info_tree, lock_at_dump=False)
