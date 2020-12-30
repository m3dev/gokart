import logging
import unittest
from unittest.mock import patch

import luigi
import pandas as pd

import gokart


class DummyModel:
    def apply(self, x):
        return x + 1

    def get(self):
        return 2


class DummyModelTask(gokart.TaskOnKart):
    task_namespace = f'{__name__}.dummy'
    rerun = True

    def run(self):
        self.dump(DummyModel())


class DummyPandasDataFrameTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.Parameter()
    rerun = True

    def run(self):
        df = pd.DataFrame(dict(x=[1, 3, 4]))
        self.dump(df)


class DummyWorkFlowWithError(gokart.TaskOnKart):
    task_namespace = __name__
    rerun = True

    def requires(self):
        return dict(model=DummyModelTask(), data_a=DummyPandasDataFrameTask(param='a'))

    def run(self):
        model: DummyModel = self.load('model')
        data = self.load_data_frame('data_a')
        data['applied'] = data['x'].apply(model.apply)
        data['y'] = data['applied'].apply(model.apply)
        self.dump(data)


class DummyWorkFlowWithoutError(gokart.TaskOnKart):
    task_namespace = __name__
    rerun = True

    def requires(self):
        return dict(model=DummyModelTask(), data_a=DummyPandasDataFrameTask(param='a'))

    def run(self):
        model: DummyModel = self.load('model')
        data = self.load_data_frame('data_a', required_columns={'x'})
        data['y'] = data['x'].apply(model.apply)
        self.dump(data)


class TestTestFrameworkForPandasDataFrame(unittest.TestCase):
    def test_run_without_error(self):
        argv = [f'{__name__}.DummyWorkFlowWithoutError', '--local-scheduler', '--test-run-pandas', '--log-level=CRITICAL', '--no-lock']
        logger = logging.getLogger('gokart.testing.check_if_run_with_empty_data_frame')
        with patch.object(logger, 'info') as mock_debug:
            with self.assertRaises(SystemExit) as exit_code:
                gokart.run(argv)
        log_str = mock_debug.call_args[0][0]
        self.assertEqual(exit_code.exception.code, 0)
        self.assertTrue('DummyModelTask' in log_str)

    def test_run_with_error(self):
        argv = [f'{__name__}.DummyWorkFlowWithError', '--local-scheduler', '--test-run-pandas', '--log-level=CRITICAL', '--no-lock']
        logger = logging.getLogger('gokart.testing.check_if_run_with_empty_data_frame')
        with patch.object(logger, 'info') as mock_debug:
            with self.assertRaises(SystemExit) as exit_code:
                gokart.run(argv)
        log_str = mock_debug.call_args[0][0]
        self.assertEqual(exit_code.exception.code, 1)
        self.assertTrue('DummyModelTask' in log_str)

    def test_run_with_namespace(self):
        argv = [
            f'{__name__}.DummyWorkFlowWithoutError', '--local-scheduler', '--test-run-pandas', f'--test-run-namespace={__name__}', '--log-level=CRITICAL',
            '--no-lock'
        ]
        logger = logging.getLogger('gokart.testing.check_if_run_with_empty_data_frame')
        with patch.object(logger, 'info') as mock_debug:
            with self.assertRaises(SystemExit) as exit_code:
                gokart.run(argv)
        log_str = mock_debug.call_args[0][0]
        self.assertEqual(exit_code.exception.code, 0)
        self.assertTrue('DummyModelTask' not in log_str)
