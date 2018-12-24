import configparser
import os
import sys
from configparser import ConfigParser

import luigi
import luigi.cmdline
import luigi.retcodes


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


def run(set_retcode=True):
    if set_retcode:
        luigi.retcodes.retcode.already_running = 10
        luigi.retcodes.retcode.missing_data = 20
        luigi.retcodes.retcode.not_run = 30
        luigi.retcodes.retcode.task_failed = 40
        luigi.retcodes.retcode.scheduling_error = 50

    _read_environ()
    _check_config()
    luigi.cmdline.luigi_run(sys.argv[1:])
