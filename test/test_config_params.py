import unittest

import gokart
import luigi
from luigi.cmdline_parser import CmdlineParser

from gokart.config_params import inherits_config_params


def in_parse(cmds, deferred_computation):
    """function copied from luigi: https://github.com/spotify/luigi/blob/e2228418eec60b68ca09a30c878ab26413846847/test/helpers.py"""
    with CmdlineParser.global_instance(cmds) as cp:
        deferred_computation(cp.get_task_obj())


class ConfigClass(luigi.Config):
    param_a = luigi.Parameter(default='config a')
    param_b = luigi.Parameter(default='config b')
    param_c = luigi.Parameter(default='config c')


@inherits_config_params(ConfigClass)
class Inherited(gokart.TaskOnKart):
    param_a = luigi.Parameter()
    param_b = luigi.Parameter(default='overrided')


class ChildTask(Inherited):
    pass


class ChildTaskWithNewParam(Inherited):
    param_new = luigi.Parameter()


class ConfigClass2(luigi.Config):
    param_a = luigi.Parameter(default='config a from config class 2')


@inherits_config_params(ConfigClass2)
class ChildTaskWithNewConfig(Inherited):
    pass


class TestInheritsConfigParam(unittest.TestCase):
    def test_inherited_params(self):
        # test fill values
        in_parse(['Inherited'], lambda task: self.assertEqual(task.param_a, 'config a'))

        # test overrided
        in_parse(['Inherited'], lambda task: self.assertEqual(task.param_b, 'config b'))

        # Command line argument takes precedence over config param
        in_parse(['Inherited', '--param-a', 'command line arg'], lambda task: self.assertEqual(task.param_a, 'command line arg'))

        # Parameters which is not a member of the task will not be set
        with self.assertRaises(AttributeError):
            in_parse(['Inherited'], lambda task: task.param_c)

    def test_child_task(self):
        in_parse(['ChildTask'], lambda task: self.assertEqual(task.param_a, 'config a'))
        in_parse(['ChildTask'], lambda task: self.assertEqual(task.param_b, 'config b'))
        in_parse(['ChildTask', '--param-a', 'command line arg'], lambda task: self.assertEqual(task.param_a, 'command line arg'))
        with self.assertRaises(AttributeError):
            in_parse(['ChildTask'], lambda task: task.param_c)

    def test_child_override(self):
        in_parse(['ChildTaskWithNewConfig'], lambda task: self.assertEqual(task.param_a, 'config a from config class 2'))
        in_parse(['ChildTaskWithNewConfig'], lambda task: self.assertEqual(task.param_b, 'config b'))
