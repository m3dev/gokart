from __future__ import annotations

import logging
import os
import sys
from logging import getLogger

import luigi
import luigi.cmdline
import luigi.cmdline_parser
import luigi.execution_summary
import luigi.interface
import luigi.retcodes
import luigi.setup_logging
from luigi.cmdline_parser import CmdlineParser

import gokart
import gokart.slack
from gokart.build import WorkerSchedulerFactory
from gokart.object_storage import ObjectStorage

logger = getLogger(__name__)


def _run_tree_info(cmdline_args, details):
    with CmdlineParser.global_instance(cmdline_args) as cp:
        gokart.tree_info().output().dump(gokart.make_tree_info(cp.get_task_obj(), details=details))


def _try_tree_info(cmdline_args):
    with CmdlineParser.global_instance(cmdline_args):
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


def _try_to_delete_unnecessary_output_file(cmdline_args: list[str]):
    with CmdlineParser.global_instance(cmdline_args) as cp:
        task = cp.get_task_obj()  # type: gokart.TaskOnKart
        if task.delete_unnecessary_output_files:
            if ObjectStorage.if_object_storage_path(task.workspace_directory):
                logger.info('delete-unnecessary-output-files is not support s3/gcs.')
            else:
                gokart.delete_local_unnecessary_outputs(task)
            sys.exit()


def _try_get_slack_api(cmdline_args: list[str]) -> gokart.slack.SlackAPI | None:
    with CmdlineParser.global_instance(cmdline_args):
        config = gokart.slack.SlackConfig()
        token = os.getenv(config.token_name, '')
        channel = config.channel
        to_user = config.to_user
        if token and channel:
            logger.info('Slack notification is activated.')
            return gokart.slack.SlackAPI(token=token, channel=channel, to_user=to_user)
    logger.info('Slack notification is not activated.')
    return None


def _try_to_send_event_summary_to_slack(slack_api: gokart.slack.SlackAPI | None, event_aggregator: gokart.slack.EventAggregator, cmdline_args: list[str]):
    if slack_api is None:
        # do nothing
        return
    options = gokart.slack.SlackConfig()
    with CmdlineParser.global_instance(cmdline_args) as cp:
        task = cp.get_task_obj()
        tree_info = gokart.make_tree_info(task, details=True) if options.send_tree_info else 'Please add SlackConfig.send_tree_info to include tree-info'
        task_name = type(task).__name__

    comment = f'Report of {task_name}' + os.linesep + event_aggregator.get_summary()
    content = os.linesep.join(['===== Event List ====', event_aggregator.get_event_list(), os.linesep, '==== Tree Info ====', tree_info])
    slack_api.send_snippet(comment=comment, title='event.txt', content=content)


def _run_with_retcodes(argv):
    """run_with_retcodes equivalent that uses gokart's WorkerSchedulerFactory."""
    retcode_logger = logging.getLogger('luigi-interface')
    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        retcodes = luigi.retcodes.retcode()

    worker = None
    try:
        worker = luigi.interface._run(argv, worker_scheduler_factory=WorkerSchedulerFactory()).worker
    except luigi.interface.PidLockAlreadyTakenExit:
        sys.exit(retcodes.already_running)
    except Exception:
        env_params = luigi.interface.core()
        luigi.setup_logging.InterfaceLogging.setup(env_params)
        retcode_logger.exception('Uncaught exception in luigi')
        sys.exit(retcodes.unhandled_exception)

    with luigi.cmdline_parser.CmdlineParser.global_instance(argv):
        task_sets = luigi.execution_summary._summary_dict(worker)
        root_task = luigi.execution_summary._root_task(worker)
        non_empty_categories = {k: v for k, v in task_sets.items() if v}.keys()

    def has(status):
        assert status in luigi.execution_summary._ORDERED_STATUSES
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


def run(cmdline_args=None, set_retcode=True):
    cmdline_args = cmdline_args or sys.argv[1:]

    if set_retcode:
        luigi.retcodes.retcode.already_running = 10
        luigi.retcodes.retcode.missing_data = 20
        luigi.retcodes.retcode.not_run = 30
        luigi.retcodes.retcode.task_failed = 40
        luigi.retcodes.retcode.scheduling_error = 50

    _try_tree_info(cmdline_args)
    _try_to_delete_unnecessary_output_file(cmdline_args)
    gokart.testing.try_to_run_test_for_empty_data_frame(cmdline_args)

    slack_api = _try_get_slack_api(cmdline_args)
    event_aggregator = gokart.slack.EventAggregator()
    try:
        event_aggregator.set_handlers()
        _run_with_retcodes(cmdline_args)
    except SystemExit as e:
        _try_to_send_event_summary_to_slack(slack_api, event_aggregator, cmdline_args)
        sys.exit(e.code)
