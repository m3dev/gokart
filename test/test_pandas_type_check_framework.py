import logging
import unittest
from logging import getLogger
from typing import Any, Dict
from unittest.mock import patch

import luigi
import pandas as pd
from luigi.mock import MockFileSystem, MockTarget

import gokart
from gokart.build import GokartBuildError
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
        luigi.setup_logging.DaemonLogging._configured = False
        luigi.setup_logging.InterfaceLogging._configured = False
        MockFileSystem().clear()
        # same way as luigi https://github.com/spotify/luigi/blob/fe7ecf4acf7cf4c084bd0f32162c8e0721567630/test/helpers.py#L175
        self._stashed_reg = luigi.task_register.Register._get_reg()

    def tearDown(self) -> None:
        luigi.setup_logging.DaemonLogging._configured = False
        luigi.setup_logging.InterfaceLogging._configured = False
        luigi.task_register.Register._set_reg(self._stashed_reg)

    @patch('sys.argv', new=['main', 'test_pandas_type_check_framework._DummyFailTask', '--log-level=CRITICAL', '--local-scheduler', '--no-lock'])
    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_fail_with_gokart_run(self):
        with self.assertRaises(SystemExit) as exit_code:
            gokart.run()
        self.assertNotEqual(exit_code.exception.code, 0)  # raise Error

    def test_fail(self):
        with self.assertRaises(GokartBuildError):
            gokart.build(_DummyFailTask(), log_level=logging.CRITICAL)

    def test_fail_with_None(self):
        with self.assertRaises(GokartBuildError):
            gokart.build(_DummyFailWithNoneTask(), log_level=logging.CRITICAL)

    def test_success(self):
        gokart.build(_DummySuccessTask())
        # no error
