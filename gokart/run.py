from __future__ import annotations

import logging
import sys
from contextlib import AbstractContextManager
from logging import getLogger
from typing import Any, cast

import luigi
import luigi.execution_summary
import luigi.interface
import luigi.retcodes
import luigi.setup_logging
from luigi.cmdline_parser import CmdlineParser

import gokart
from gokart.build import WorkerSchedulerFactory
from gokart.object_storage import ObjectStorage

logger = getLogger(__name__)


def _global_instance(cmdline_args: list[str]) -> AbstractContextManager[CmdlineParser]:
    # luigi's CmdlineParser.global_instance is untyped
    return cast(AbstractContextManager[CmdlineParser], cast(Any, CmdlineParser).global_instance(cmdline_args))


def _get_task_obj(cp: CmdlineParser) -> gokart.TaskOnKart[Any]:
    return cast(gokart.TaskOnKart[Any], cp.get_task_obj())


def _run_tree_info(cmdline_args: list[str], details: bool):
    with _global_instance(cmdline_args) as cp:
        gokart.tree_info().output().dump(gokart.make_tree_info(_get_task_obj(cp), details=details))


def _try_tree_info(cmdline_args: list[str]):
    with _global_instance(cmdline_args):
        mode = gokart.tree_info().mode
        output_path = gokart.tree_info().output().path()

    # do nothing if `mode` is empty.
    if mode == '':
        return

    # output tree info and exit.
    if mode == 'simple':
        _run_tree_info(cmdline_args, details=False)
    elif mode == 'all':
        _run_tree_info(cmdline_args, details=True)
    else:
        raise ValueError(f'--tree-info-mode must be "simple" or "all", but "{mode}" is passed.')
    logger.info(f'output tree info: {output_path}')
    sys.exit()


def _try_to_delete_unnecessary_output_file(cmdline_args: list[str]) -> None:
    with _global_instance(cmdline_args) as cp:
        task = _get_task_obj(cp)
        if task.delete_unnecessary_output_files:
            if ObjectStorage.if_object_storage_path(task.workspace_directory):
                logger.info('delete-unnecessary-output-files is not support s3/gcs.')
            else:
                gokart.delete_local_unnecessary_outputs(task)
            sys.exit()


def _run_with_retcodes(argv: list[str]):
    """run_with_retcodes equivalent that uses gokart's WorkerSchedulerFactory."""
    retcode_logger = logging.getLogger('luigi-interface')
    with _global_instance(argv):
        retcodes = luigi.retcodes.retcode()

    worker = None
    try:
        worker = cast(Any, luigi.interface)._run(argv, worker_scheduler_factory=WorkerSchedulerFactory()).worker
    except luigi.interface.PidLockAlreadyTakenExit:
        sys.exit(retcodes.already_running)
    except Exception:
        env_params = luigi.interface.core()
        luigi.setup_logging.InterfaceLogging.setup(cast(Any, env_params))
        retcode_logger.exception('Uncaught exception in luigi')
        sys.exit(retcodes.unhandled_exception)

    with _global_instance(argv):
        task_sets = cast(dict[str, Any], cast(Any, luigi.execution_summary)._summary_dict(worker))
        root_task = cast(Any, luigi.execution_summary)._root_task(worker)
        non_empty_categories = {k: v for k, v in task_sets.items() if v}.keys()

    def has(status: str):
        assert status in luigi.execution_summary._ORDERED_STATUSES  # pyright: ignore[reportPrivateUsage]
        return status in non_empty_categories

    codes_and_conds = (
        (retcodes.missing_data, has('still_pending_ext')),
        (retcodes.task_failed, has('failed')),
        (retcodes.already_running, has('run_by_other_worker')),
        (retcodes.scheduling_error, has('scheduling_error')),
        (retcodes.not_run, has('not_run')),
    )
    expected_ret_code = max(code * (1 if cond else 0) for code, cond in codes_and_conds)

    if expected_ret_code == 0 and root_task not in task_sets['completed'] and root_task not in task_sets['already_done']:
        sys.exit(retcodes.not_run)
    else:
        sys.exit(expected_ret_code)


def run(cmdline_args: list[str] | None = None, set_retcode: bool = True):
    cmdline_args = cmdline_args or sys.argv[1:]

    if set_retcode:
        luigi.retcodes.retcode.already_running = 10
        luigi.retcodes.retcode.missing_data = 20
        luigi.retcodes.retcode.not_run = 30
        luigi.retcodes.retcode.task_failed = 40
        luigi.retcodes.retcode.scheduling_error = 50

    _try_tree_info(cmdline_args)
    _try_to_delete_unnecessary_output_file(cmdline_args)
    # imported lazily because gokart.testing imports gokart, which would make a circular import at module level
    from gokart.testing import try_to_run_test_for_empty_data_frame

    try_to_run_test_for_empty_data_frame(cmdline_args)

    _run_with_retcodes(cmdline_args)
