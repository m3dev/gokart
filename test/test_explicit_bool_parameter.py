import unittest
import argparse
from unittest.mock import patch

import luigi
import luigi.mock
from luigi.cmdline_parser import CmdlineParser

import gokart


def run_locally(args):
    temp = CmdlineParser._instance
    try:
        CmdlineParser._instance = None
        run_exit_status = luigi.run(['--local-scheduler', '--no-lock'] + args)
    finally:
        CmdlineParser._instance = temp
    return run_exit_status


class WithDefaultTrue(gokart.TaskOnKart):
    param = gokart.parameter.ExplicitBoolParameter(default=True)


class WithDefaultFalse(gokart.TaskOnKart):
    param = gokart.parameter.ExplicitBoolParameter(default=False)


class ExplicitParsing(gokart.TaskOnKart):
    param = gokart.parameter.ExplicitBoolParameter()

    def run(self):
        ExplicitParsing._param = self.param


class TestExplicitBoolParameter(unittest.TestCase):
    def test_bool_default(self):
        self.assertTrue(WithDefaultTrue().param)
        self.assertFalse(WithDefaultFalse().param)

    def test_parse_param(self):
        run_locally(['ExplicitParsing', '--param', 'true'])
        self.assertTrue(ExplicitParsing._param)
        run_locally(['ExplicitParsing', '--param', 'false'])
        self.assertFalse(ExplicitParsing._param)
        run_locally(['ExplicitParsing', '--param', 'True'])
        self.assertTrue(ExplicitParsing._param)
        run_locally(['ExplicitParsing', '--param', 'False'])
        self.assertFalse(ExplicitParsing._param)

    def test_missing_parameter(self):
        with self.assertRaises(luigi.parameter.MissingParameterException):
            run_locally(['ExplicitParsing'])

    def test_value_error(self):
        with self.assertRaises(ValueError):
            run_locally(['ExplicitParsing', '--param', 'Foo'])

    def test_expected_one_argment_error(self):
        # argparse throw "expected one argument" error
        with self.assertRaises(SystemExit):
            run_locally(['ExplicitParsing', '--param'])
