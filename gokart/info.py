from __future__ import annotations

from logging import getLogger

import luigi

from gokart.task import TaskOnKart
from gokart.tree.task_info import make_task_info_as_tree_str

logger = getLogger(__name__)


def make_tree_info(
    task: TaskOnKart,
    indent: str = '',
    last: bool = True,
    details: bool = False,
    abbr: bool = True,
    visited_tasks: set[str] | None = None,
    ignore_task_names: list[str] | None = None,
) -> str:
    """
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format

    This function has moved to `gokart.tree.task_info.make_task_info_as_tree_str`.
    This code is remained for backward compatibility.

    Parameters
    ----------
    - task: TaskOnKart
        Root task.
    - details: bool
        Whether or not to output details.
    - abbr: bool
        Whether or not to simplify tasks information that has already appeared.
    - ignore_task_names: list[str] | None
        List of task names to ignore.
    Returns
    -------
    - tree_info : str
        Formatted task dependency tree.
    """
    return make_task_info_as_tree_str(task=task, details=details, abbr=abbr, ignore_task_names=ignore_task_names)


class tree_info(TaskOnKart):
    mode: str = luigi.Parameter(default='', description='This must be in ["simple", "all"].')
    output_path: str = luigi.Parameter(default='tree.txt', description='Output file path.')

    def output(self):
        return self.make_target(self.output_path, use_unique_id=False)
