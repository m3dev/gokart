import unittest
from unittest.mock import patch

import luigi
import luigi.mock

import gokart


class _SubDummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()

    def run(self):
        self.dump('test')


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    sub_task = gokart.TaskInstanceParameter()

    def output(self):
        return self.make_target('test.txt')

    def run(self):
        self.dump('test')


class RestoreTaskByIDTest(unittest.TestCase):
    def setUp(self) -> None:
        luigi.mock.MockFileSystem().clear()

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: luigi.mock.MockTarget(path, **kwargs))
    def test(self):
        task = _DummyTask(sub_task=_SubDummyTask(param=10))
        luigi.build([task], local_scheduler=True, log_level="CRITICAL")

        unique_id = task.make_unique_id()
        restored = _DummyTask.restore(unique_id)
        self.assertTrue(task.make_unique_id(), restored.make_unique_id())


if __name__ == '__main__':
    unittest.main()
