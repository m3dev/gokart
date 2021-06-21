import os
import unittest

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
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

    def test_build(self):
        text = 'test'
        output = gokart.build(_DummyTask(param=text), reset_register=False)
        self.assertEqual(output, text)

    def test_read_config(self):
        os.environ.setdefault('test_param', 'test')
        config_file_path = os.path.join(os.path.dirname(__name__), 'config', 'test_config.ini')
        gokart.utils.add_config(config_file_path)
        output = gokart.build(_DummyTask())
        self.assertEqual(output, 'test')


if __name__ == '__main__':
    unittest.main()
