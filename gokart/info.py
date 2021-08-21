import warnings
from logging import getLogger
from typing import List, NamedTuple, Optional, Set, Tuple

import luigi

from gokart.task import TaskOnKart
from gokart.tree.task_info import make_tree_info_string

logger = getLogger(__name__)


def make_tree_info(task, indent='', last=True, details=False, abbr=True, visited_tasks=None, ignore_task_names=None):
    """
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format

    This function has moved to `gokart.tree.task_info.make_tree_info_string`.
    This code is remained for backward compatibility.
    """
    return make_tree_info_string(task=task, details=details, abbr=abbr, ignore_task_names=ignore_task_names)


class tree_info(TaskOnKart):
    mode = luigi.Parameter(default='', description='This must be in ["simple", "all"].')  # type: str
    output_path = luigi.Parameter(default='tree.txt', description='Output file path.')  # type: str

    def output(self):
        return self.make_target(self.output_path, use_unique_id=False)
