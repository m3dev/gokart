import os
import sys
from logging import getLogger
from typing import List, Optional

import luigi
import luigi.cmdline
import luigi.retcodes
from luigi.cmdline_parser import CmdlineParser

import gokart
import gokart.slack
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


def _try_to_delete_unnecessary_output_file(cmdline_args: List[str]):
    with CmdlineParser.global_instance(cmdline_args) as cp:
        task = cp.get_task_obj()  # type: gokart.TaskOnKart
        if task.delete_unnecessary_output_files:
            if ObjectStorage.if_object_storage_path(task.workspace_directory):
                logger.info('delete-unnecessary-output-files is not support s3/gcs.')
            else:
                gokart.delete_local_unnecessary_outputs(task)
            sys.exit()


def _try_get_slack_api(cmdline_args: List[str]) -> Optional[gokart.slack.SlackAPI]:
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


def _try_to_send_event_summary_to_slack(slack_api: Optional[gokart.slack.SlackAPI], event_aggregator: gokart.slack.EventAggregator, cmdline_args: List[str]):
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
        luigi.cmdline.luigi_run(cmdline_args)
    except SystemExit as e:
        _try_to_send_event_summary_to_slack(slack_api, event_aggregator, cmdline_args)
        sys.exit(e.code)
