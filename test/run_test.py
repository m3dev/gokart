import os
import unittest

import luigi
from unittest.mock import patch

import gokart


class _DummyTask(gokart.TaskOnKart):
    param = luigi.Parameter()


class RunTest(unittest.TestCase):
    def setUp(self):
        luigi.configuration.LuigiConfigParser._instance = None
        os.environ.clear()

    @patch('sys.argv', new=['main', '_DummyTask', '--log-level=CRITICAL', '--local-scheduler'])
    def test_run(self):
        config_file_path = os.path.join(os.path.dirname(__name__), 'test_config.ini')
        luigi.configuration.LuigiConfigParser.add_config_path(config_file_path)
        os.environ.setdefault('test_param', 'test')
        with self.assertRaises(SystemExit) as exit_code:
            gokart.run()
        self.assertEqual(exit_code.exception.code, 0)

    @patch('sys.argv', new=['main', '_DummyTask', '--log-level=CRITICAL', '--local-scheduler'])
    def test_run_with_undefined_environ(self):
        config_file_path = os.path.join(os.path.dirname(__name__), 'test_config.ini')
        luigi.configuration.LuigiConfigParser.add_config_path(config_file_path)
        with self.assertRaises(luigi.parameter.MissingParameterException) as missing_parameter:
            gokart.run()


if __name__ == '__main__':
    unittest.main()
