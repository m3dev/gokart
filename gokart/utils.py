import configparser
import os
import re
from configparser import BasicInterpolation, Interpolation

import luigi
from luigi.configuration.cfg_parser import CombinedInterpolation, EnvironmentInterpolation


class BasicEnvironmentInterpolation(Interpolation):
    """
    Custom interpolation which allows values to refer to environment variables
    using the ``%(ENVVAR)s`` syntax.
    Based on luigi.configulation.cfg_parser.EnvironmentInterpolation.
    """
    _ENVRE = re.compile(r"%\(([^\)]+)\)s")  # matches "%(envvar)s"

    def before_get(self, parser, section, option, value, defaults):
        return self._interpolate_env(option, section, value)

    def _interpolate_env(self, option, section, value):
        parts = []
        while value:
            match = self._ENVRE.search(value)
            if match is None:
                parts.append(value)
                break
            envvar = match.groups()[0]
            if envvar in os.environ:
                envval = os.environ[envvar]
                start, end = match.span()
                parts.append(value[:start])
                parts.append(envval)
                value = value[end:]
            else:
                parts.append(value)
                break
        return "".join(parts)


def set_environ_interpolation():
    config = luigi.configuration.get_config()
    config._interpolation = CombinedInterpolation([BasicEnvironmentInterpolation(), BasicInterpolation(), EnvironmentInterpolation()])


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
    set_environ_interpolation()
