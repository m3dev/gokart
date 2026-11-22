from __future__ import annotations

import itertools
import os
import pathlib
from logging import getLogger
from typing import Any

import gokart
from gokart.target import is_persistent_target, target_path
from gokart.utils import flatten

logger = getLogger(__name__)


def _get_all_output_file_paths(task: gokart.TaskOnKart[Any]) -> list[str]:
    # WHY: an in-memory key looks like a file path, but the in-memory output does not use the file there.
    output_paths = [target_path(t) for t in flatten(task.output()) if is_persistent_target(t)]
    children = flatten(task.requires())
    output_paths.extend(itertools.chain.from_iterable([_get_all_output_file_paths(child) for child in children]))
    return output_paths


def delete_local_unnecessary_outputs(task: gokart.TaskOnKart[Any]) -> None:
    task.make_unique_id()  # this is required to make unique ids.
    all_files = {str(path) for path in pathlib.Path(task.workspace_directory).rglob('*.*')}
    log_files = {str(path) for path in pathlib.Path(os.path.join(task.workspace_directory, 'log')).rglob('*.*')}
    necessary_files = set(_get_all_output_file_paths(task))
    unnecessary_files = all_files - necessary_files - log_files
    if len(unnecessary_files) == 0:
        logger.info('all files are necessary for this task.')
    else:
        logger.info(f'remove following files: {os.linesep} {os.linesep.join(unnecessary_files)}')
    for file in unnecessary_files:
        os.remove(file)
