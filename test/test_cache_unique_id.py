import os
import unittest

import luigi
import luigi.mock

import gokart


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__

    def requires(self):
        return _DummyTaskDep()

    def run(self):
        self.dump(self.load())


class _DummyTaskDep(gokart.TaskOnKart):
    task_namespace = __name__

    param = luigi.Parameter()

    def run(self):
        self.dump(self.param)


class RunTest(unittest.TestCase):
    def setUp(self):
        luigi.configuration.LuigiConfigParser._instance = None
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

        config_file_path = os.path.join(os.path.dirname(__name__), 'config', 'test_config.ini')
        luigi.configuration.LuigiConfigParser.add_config_path(config_file_path)
        os.environ.setdefault('test_param', 'original_param')

    def test_cache_unique_id_true(self):
        output1 = gokart.build(_DummyTask(cache_unique_id=True))

        os.environ['test_param'] = 'updated_param'
        output2 = gokart.build(_DummyTask(cache_unique_id=True))

        self.assertEqual(output1, output2)

    def test_cache_unique_id_false(self):
        output1 = gokart.build(_DummyTask(cache_unique_id=False))

        os.environ['test_param'] = 'updated_param'
        output2 = gokart.build(_DummyTask(cache_unique_id=False))

        self.assertNotEqual(output1, output2)


if __name__ == '__main__':
    unittest.main()
