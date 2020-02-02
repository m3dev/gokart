import unittest
from logging import getLogger
from typing import Dict, Any

import pandas as pd
from luigi.mock import MockTarget, MockFileSystem
from mock import patch

import gokart
from gokart.pandas_type_config import PandasTypeConfig

logger = getLogger(__name__)


class TestPandasTypeConfig(PandasTypeConfig):
    task_namespace = 'test_pandas_type_check_framework'

    @classmethod
    def type_dict(cls) -> Dict[str, Any]:
        return {'system_cd': int}


class _DummyFailTask(gokart.TaskOnKart):
    task_namespace = 'test_pandas_type_check_framework'
    rerun = True

    def output(self):
        return self.make_target('dummy.pkl')

    def run(self):
        df = pd.DataFrame(dict(system_cd=['1']))
        self.dump(df)


class _DummyFailWithNoneTask(gokart.TaskOnKart):
    task_namespace = 'test_pandas_type_check_framework'
    rerun = True

    def output(self):
        return self.make_target('dummy.pkl')

    def run(self):
        df = pd.DataFrame(dict(system_cd=[1, None]))
        self.dump(df)


class _DummySuccessTask(gokart.TaskOnKart):
    task_namespace = 'test_pandas_type_check_framework'
    rerun = True

    def output(self):
        return self.make_target('dummy.pkl')

    def run(self):
        df = pd.DataFrame(dict(system_cd=[1]))
        self.dump(df)


class TestPandasTypeCheckFramework(unittest.TestCase):
    def setUp(self) -> None:
        MockFileSystem().clear()

    @patch('sys.argv', new=['main', 'test_pandas_type_check_framework._DummyFailTask', '--log-level=CRITICAL', '--local-scheduler', '--no-lock'])
    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_fail(self):
        with self.assertRaises(SystemExit) as exit_code:
            gokart.run()
        self.assertNotEqual(exit_code.exception.code, 0)  # raise Error

    @patch('sys.argv', new=['main', 'test_pandas_type_check_framework._DummyFailWithNoneTask', '--log-level=CRITICAL', '--local-scheduler', '--no-lock'])
    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_fail_with_None(self):
        with self.assertRaises(SystemExit) as exit_code:
            gokart.run()
        self.assertNotEqual(exit_code.exception.code, 0)  # raise Error

    @patch('sys.argv', new=['main', 'test_pandas_type_check_framework._DummySuccessTask', '--log-level=CRITICAL', '--local-scheduler', '--no-lock'])
    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_success(self):
        with self.assertRaises(SystemExit) as exit_code:
            gokart.run()
        self.assertEqual(exit_code.exception.code, 0)
