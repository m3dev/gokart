import itertools
import os
import pathlib
from logging import getLogger

import luigi

import gokart

logger = getLogger(__name__)


def _get_all_output_file_paths(task: gokart.TaskOnKart):
    output_paths = [t.path() for t in luigi.task.flatten(task.output())]
    children = luigi.task.flatten(task.requires())
    output_paths.extend(itertools.chain.from_iterable([_get_all_output_file_paths(child) for child in children]))
    return output_paths


def delete_local_unnecessary_outputs(task: gokart.TaskOnKart):
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
