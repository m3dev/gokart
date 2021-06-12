import logging
import os
import unittest

import luigi
import luigi.mock

import gokart
from gokart.build import LoggerConfig


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.Parameter()

    def output(self):
        return self.make_target('./test/dummy.pkl')

    def run(self):
        self.dump(self.param)


class RunTest(unittest.TestCase):
    def setUp(self):
        luigi.configuration.LuigiConfigParser._instance = None
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

    def test_build(self):
        text = 'test'
        output = gokart.build(_DummyTask(param=text), reset_register=False)
        self.assertEqual(output, text)


class LoggerConfigTest(unittest.TestCase):
    def test_logger_config(self):
        verbose = False
        level = None
        for verbose, level, expected in ((False, None, logging.CRITICAL), (True, None, logging.NOTSET), (False, logging.DEBUG, logging.DEBUG),
                                         (True, logging.DEBUG, logging.DEBUG)):
            with self.subTest(verbose=verbose, level=level, expected=expected):
                with LoggerConfig(verbose, level) as lc:
                    self.assertEqual(lc.logger.level, expected)


if __name__ == '__main__':
    unittest.main()
