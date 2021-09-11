import logging
import sys
from typing import List, Optional

import luigi
import pandas as pd
from luigi.cmdline_parser import CmdlineParser

import gokart

test_logger = logging.getLogger(__name__)
test_logger.addHandler(logging.StreamHandler())
test_logger.setLevel(logging.INFO)


class test_run(gokart.TaskOnKart):
    pandas: bool = luigi.BoolParameter()
    namespace: Optional[str] = luigi.OptionalParameter(default=None,
                                                       description='When task namespace is not defined explicitly, please use "__not_user_specified".')


class _TestStatus:
    def __init__(self, task: gokart.TaskOnKart) -> None:
        self.namespace = task.task_namespace
        self.name = type(task).__name__
        self.task_id = task.make_unique_id()
        self.status = 'OK'
        self.message = None

    def format(self) -> str:
        s = f'status={self.status}; namespace={self.namespace}; name={self.name}; id={self.task_id};'
        if self.message:
            s += f' message={type(self.message)}: {", ".join(map(str, self.message.args))}'
        return s

    def fail(self) -> bool:
        return self.status != 'OK'


def _get_all_tasks(task: gokart.TaskOnKart) -> List[gokart.TaskOnKart]:
    return luigi.task.flatten([_get_all_tasks(o) for o in luigi.task.flatten(task.requires()) if isinstance(o, gokart.TaskOnKart)] + [task])


def _run_with_test_status(task: gokart.TaskOnKart):
    test_message = _TestStatus(task)
    try:
        task.run()
    except Exception as e:
        test_message.status = 'NG'
        test_message.message = e
    return test_message


def _test_run_with_empty_data_frame(cmdline_args: List[str], test_run_params: test_run):
    from unittest.mock import patch

    try:
        gokart.run(cmdline_args=cmdline_args)
    except SystemExit as e:
        assert e.code == 0, f'original workflow does not run properly. It exited with error code {e}.'

    with CmdlineParser.global_instance(cmdline_args) as cp:
        all_tasks = _get_all_tasks(cp.get_task_obj())

    if test_run_params.namespace is not None:
        all_tasks = [t for t in all_tasks if t.task_namespace == test_run_params.namespace]

    with patch('gokart.TaskOnKart.load_data_frame', new=lambda *args, required_columns=None, **kwargs: pd.DataFrame(columns=required_columns)):
        with patch('gokart.TaskOnKart.dump', new=lambda *args, **kwargs: None):
            test_status_list = [_run_with_test_status(t) for t in all_tasks]

    test_logger.info('gokart test results:\n' + '\n'.join(s.format() for s in test_status_list))
    if any(s.fail() for s in test_status_list):
        sys.exit(1)


def try_to_run_test_for_empty_data_frame(cmdline_args: List[str]):
    with CmdlineParser.global_instance(cmdline_args):
        test_run_params = test_run()

    if test_run_params.pandas:
        cmdline_args = [a for a in cmdline_args if not a.startswith('--test-run-')]
        _test_run_with_empty_data_frame(cmdline_args=cmdline_args, test_run_params=test_run_params)
        sys.exit(0)
