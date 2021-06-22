import configparser
import os
from configparser import ConfigParser

import luigi


def read_environ():
    config = luigi.configuration.get_config()
    for key, value in os.environ.items():
        super(ConfigParser, config).set(section=None, option=key, value=value.replace('%', '%%'))


def check_config():
    parser = luigi.configuration.LuigiConfigParser.instance()
    for section in parser.sections():
        try:
            parser.items(section)
        except configparser.InterpolationMissingOptionError as e:
            raise luigi.parameter.MissingParameterException(f'Environment variable "{e.args[3]}" must be set.')


def add_config(file_path: str):
    _, ext = os.path.splitext(file_path)
    luigi.configuration.core.parser = ext
    assert luigi.configuration.add_config_path(file_path)
    read_environ()
