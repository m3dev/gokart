import io
import logging
import os
import sys
import unittest
from copy import copy
from typing import Dict

if sys.version_info >= (3, 11):
    from typing import assert_type
else:
    from typing_extensions import assert_type

import luigi
import luigi.mock

import gokart
from gokart.build import GokartBuildError, LoggerConfig, TaskDumpConfig, TaskDumpMode, TaskDumpOutputType, process_task_info
from gokart.conflict_prevention_lock.task_lock import TaskLockException


class _DummyTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    param: str = luigi.Parameter()

    def output(self):
        return self.make_target('./test/dummy.pkl')

    def run(self):
        self.dump(self.param)


class _DummyTaskTwoOutputs(gokart.TaskOnKart[Dict[str, str]]):
    task_namespace = __name__
    param1: str = luigi.Parameter()
    param2: str = luigi.Parameter()

    def output(self):
        return {'out1': self.make_target('./test/dummy1.pkl'), 'out2': self.make_target('./test/dummy2.pkl')}

    def run(self):
        self.dump(self.param1, 'out1')
        self.dump(self.param2, 'out2')


class _DummyFailedTask(gokart.TaskOnKart):
    task_namespace = __name__

    def run(self):
        raise RuntimeError


class _ParallelRunner(gokart.TaskOnKart[str]):
    def requires(self):
        return [_DummyTask(param=str(i)) for i in range(10)]

    def run(self):
        self.dump('done')


class _LoadRequires(gokart.TaskOnKart[str]):
    task: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()

    def requires(self):
        return self.task

    def run(self):
        s = self.load(self.task)
        self.dump(s)


class RunTest(unittest.TestCase):
    def setUp(self):
        luigi.setup_logging.DaemonLogging._configured = False
        luigi.setup_logging.InterfaceLogging._configured = False
        luigi.configuration.LuigiConfigParser._instance = None
        self.config_paths = copy(luigi.configuration.LuigiConfigParser._config_paths)
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

    def tearDown(self):
        luigi.configuration.LuigiConfigParser._config_paths = self.config_paths
        os.environ.clear()
        luigi.setup_logging.DaemonLogging._configured = False
        luigi.setup_logging.InterfaceLogging._configured = False

    def test_build(self):
        text = 'test'
        output = gokart.build(_DummyTask(param=text), reset_register=False)
        self.assertEqual(output, text)

    # def test_build_parallel(self):
    #     output = gokart.build(_ParallelRunner(), reset_register=False, workers=20)
    #     self.assertEqual(output, 'done')

    def test_read_config(self):
        class _DummyTask(gokart.TaskOnKart):
            task_namespace = "test_read_config"
            param = luigi.Parameter()
            def run(self):
                self.dump(self.param)

        os.environ.setdefault('test_param', 'test')
        config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'test_config.ini')
        gokart.utils.add_config(config_file_path)
        output = gokart.build(_DummyTask(), reset_register=False)
        assert_type(output, str)
        self.assertEqual(output, 'test')

    def test_build_dict_outputs(self):
        param_dict = {
            'out1': 'test1',
            'out2': 'test2',
        }
        output = gokart.build(_DummyTaskTwoOutputs(param1=param_dict['out1'], param2=param_dict['out2']), reset_register=False)
        assert_type(output, Dict[str, str])
        self.assertEqual(output, param_dict)

    def test_failed_task(self):
        with self.assertRaises(GokartBuildError):
            gokart.build(_DummyFailedTask(), reset_register=False, log_level=logging.CRITICAL)

    def test_load_requires(self):
        text = 'test'
        output = gokart.build(_LoadRequires(task=_DummyTask(param=text)), reset_register=False)
        self.assertEqual(output, text)


class LoggerConfigTest(unittest.TestCase):
    def test_logger_config(self):
        for level, enable_expected, disable_expected in (
            (logging.INFO, logging.INFO, logging.DEBUG),
            (logging.DEBUG, logging.DEBUG, logging.NOTSET),
            (logging.CRITICAL, logging.CRITICAL, logging.ERROR),
        ):
            with self.subTest(level=level, enable_expected=enable_expected, disable_expected=disable_expected):
                with LoggerConfig(level) as lc:
                    self.assertTrue(lc.logger.isEnabledFor(enable_expected))
                    self.assertTrue(not lc.logger.isEnabledFor(disable_expected))


class ProcessTaskInfoTest(unittest.TestCase):
    def test_process_task_info(self):
        task = _DummyTask(param='test')
        for config in (
            TaskDumpConfig(mode=TaskDumpMode.TREE, output_type=TaskDumpOutputType.PRINT),
            TaskDumpConfig(mode=TaskDumpMode.TABLE, output_type=TaskDumpOutputType.PRINT),
        ):
            with LoggerConfig(level=logging.INFO):
                from gokart.build import logger
                log_stream = io.StringIO()
                handler = logging.StreamHandler(log_stream)

                handler.setLevel(logging.INFO)
                logger.addHandler(handler)
                process_task_info(task, config)
                logger.removeHandler(handler)
                handler.close()

                self.assertIn(
                    member=str(task.make_unique_id()), 
                    container=log_stream.getvalue()
                )

class _FailThreeTimesAndSuccessTask(gokart.TaskOnKart):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_counter = 0

    def run(self):
        if self.failed_counter < 3:
            self.failed_counter += 1
            raise TaskLockException()
        self.dump('done')


class TestBuildHasLockedTaskException(unittest.TestCase):
    def test_build_expo_backoff_when_luigi_failed_due_to_locked_task(self):
        gokart.build(_FailThreeTimesAndSuccessTask(), reset_register=False)


if __name__ == '__main__':
    unittest.main()
