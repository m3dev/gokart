import os
import unittest

import luigi
import luigi.mock

import gokart


class _DummyTask(gokart.TaskOnKart):
    def requires(self):
        return _DummyTaskDep()

    def run(self):
        self.dump(self.load())


class _DummyTaskDep(gokart.TaskOnKart):
    param = luigi.Parameter()

    def run(self):
        self.dump(self.param)


class CacheUniqueIDTest(unittest.TestCase):
    def setUp(self):
        luigi.configuration.LuigiConfigParser._instance = None
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

    def test_cache_unique_id_true(self):
        _DummyTaskDep.param = luigi.Parameter(default='original_param')

        output1 = gokart.build(_DummyTask(cache_unique_id=True), reset_register=False)

        _DummyTaskDep.param = luigi.Parameter(default='updated_param')
        output2 = gokart.build(_DummyTask(cache_unique_id=True), reset_register=False)
        self.assertEqual(output1, output2)

    def test_cache_unique_id_false(self):
        _DummyTaskDep.param = luigi.Parameter(default='original_param')

        output1 = gokart.build(_DummyTask(cache_unique_id=False), reset_register=False)

        _DummyTaskDep.param = luigi.Parameter(default='updated_param')
        output2 = gokart.build(_DummyTask(cache_unique_id=False), reset_register=False)
        self.assertNotEqual(output1, output2)


if __name__ == '__main__':
    unittest.main()
