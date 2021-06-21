import os
import unittest
from copy import copy

import luigi
import luigi.mock

import gokart


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
        self.config_paths = copy(luigi.configuration.LuigiConfigParser._config_paths)
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()
    
    def tearDown(self) -> None:
        luigi.configuration.LuigiConfigParser._config_paths = self.config_paths
        os.environ.clear()

    def test_build(self):
        text = 'test'
        output = gokart.build(_DummyTask(param=text), reset_register=False)
        self.assertEqual(output, text)

    def test_read_config(self):
        os.environ.setdefault('test_param', 'test')
        config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config', 'test_config.ini')
        gokart.utils.add_config(config_file_path)
        output = gokart.build(_DummyTask(), reset_register=False)
        self.assertEqual(output, 'test')


if __name__ == '__main__':
    unittest.main()
