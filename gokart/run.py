import configparser
import os
import sys
from configparser import ConfigParser
from logging import getLogger
from typing import List

import luigi
import luigi.cmdline
import luigi.retcodes
from luigi.cmdline_parser import CmdlineParser

import gokart

logger = getLogger(__name__)


def _read_environ():
    config = luigi.configuration.get_config()
    for key, value in os.environ.items():
        super(ConfigParser, config).set(section=None, option=key, value=value.replace('%', '%%'))


def _check_config():
    parser = luigi.configuration.LuigiConfigParser.instance()
    for section in parser.sections():
        try:
            parser.items(section)
        except configparser.InterpolationMissingOptionError as e:
            raise luigi.parameter.MissingParameterException(f'Environment variable "{e.args[3]}" must be set.')


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
    exit()


def _try_delete_unnecessary_output_file(cmdline_args: List[str]):
    with CmdlineParser.global_instance(cmdline_args) as cp:
        task = cp.get_task_obj()  # type: gokart.TaskOnKart
        if task.delete_unnecessary_output_files:
            if task.workspace_directory.startswith('s3://'):
                logger.info('delete-unnecessary-output-files is not support s3.')
            else:
                gokart.delete_local_unnecessary_outputs(task)
            exit()


def run(cmdline_args=None, set_retcode=True):
    cmdline_args = cmdline_args or sys.argv[1:]

    if set_retcode:
        luigi.retcodes.retcode.already_running = 10
        luigi.retcodes.retcode.missing_data = 20
        luigi.retcodes.retcode.not_run = 30
        luigi.retcodes.retcode.task_failed = 40
        luigi.retcodes.retcode.scheduling_error = 50

    _read_environ()
    _check_config()
    _try_tree_info(cmdline_args)
    _try_delete_unnecessary_output_file(cmdline_args)

    luigi.cmdline.luigi_run(cmdline_args)


