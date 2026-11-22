from __future__ import annotations

import itertools
import os
import pathlib
from logging import getLogger
from typing import Any

import gokart
from gokart.target import is_persistent_target
from gokart.utils import flatten

logger = getLogger(__name__)


def _get_all_output_file_paths(task: gokart.TaskOnKart[Any]) -> list[str]:
    # WHY: an in-memory key looks like a file path, but the in-memory output does not use the file there.
    output_paths = [t.path() for t in flatten(task.output()) if is_persistent_target(t)]
    children = flatten(task.requires())
    output_paths.extend(itertools.chain.from_iterable([_get_all_output_file_paths(child) for child in children]))
    return output_paths


def delete_local_unnecessary_outputs(task: gokart.TaskOnKart[Any]) -> None:
    task.make_unique_id()  # this is required to make unique ids.
    # WHY: rglob drops a leading './' (the default workspace_directory is './resources/') and tasks may write the same directory
    # differently, so the directories are resolved before comparing. rglob does not descend into symlinked directories, so the paths
    # under the resolved workspace are already resolved; only the file name is kept as is, so that a stale symlink is still deleted.
    workspace = pathlib.Path(task.workspace_directory).resolve()
    all_files = {str(path) for path in workspace.rglob('*.*') if not path.is_dir()}
    log_files = {str(path) for path in (workspace / 'log').rglob('*.*')}
    necessary_files = {os.path.join(os.path.realpath(os.path.dirname(path)), os.path.basename(path)) for path in _get_all_output_file_paths(task)}
    unnecessary_files = all_files - necessary_files - log_files
    if len(unnecessary_files) == 0:
        logger.info('all files are necessary for this task.')
    else:
        logger.info(f'remove following files: {os.linesep} {os.linesep.join(unnecessary_files)}')
    for file in unnecessary_files:
        os.remove(file)
