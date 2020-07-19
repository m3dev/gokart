import unittest

import luigi
import luigi.mock
from luigi.cmdline_parser import CmdlineParser

import gokart


def in_parse(cmds, deferred_computation):
    with CmdlineParser.global_instance(cmds) as cp:
        deferred_computation(cp.get_task_obj())


class WithDefaultTrue(gokart.TaskOnKart):
    param = gokart.ExplicitBoolParameter(default=True)


class WithDefaultFalse(gokart.TaskOnKart):
    param = gokart.ExplicitBoolParameter(default=False)


class ExplicitParsing(gokart.TaskOnKart):
    param = gokart.ExplicitBoolParameter()

    def run(self):
        ExplicitParsing._param = self.param


class TestExplicitBoolParameter(unittest.TestCase):
    def test_bool_default(self):
        self.assertTrue(WithDefaultTrue().param)
        self.assertFalse(WithDefaultFalse().param)

    def test_parse_param(self):
        in_parse(['ExplicitParsing', '--param', 'true'], lambda task: self.assertTrue(task.param))
        in_parse(['ExplicitParsing', '--param', 'false'], lambda task: self.assertFalse(task.param))
        in_parse(['ExplicitParsing', '--param', 'True'], lambda task: self.assertTrue(task.param))
        in_parse(['ExplicitParsing', '--param', 'False'], lambda task: self.assertFalse(task.param))

    def test_missing_parameter(self):
        with self.assertRaises(luigi.parameter.MissingParameterException):
            in_parse(['ExplicitParsing'], lambda: True)

    def test_value_error(self):
        with self.assertRaises(ValueError):
            in_parse(['ExplicitParsing', '--param', 'Foo'], lambda: True)

    def test_expected_one_argment_error(self):
        # argparse throw "expected one argument" error
        with self.assertRaises(SystemExit):
            in_parse(['ExplicitParsing', '--param'], lambda: True)
