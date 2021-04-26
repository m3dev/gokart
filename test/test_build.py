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


if __name__ == '__main__':
    unittest.main()
