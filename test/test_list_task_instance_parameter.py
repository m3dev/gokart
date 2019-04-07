import unittest

import luigi

import gokart
from gokart import TaskOnKart


class _DummySubTask(TaskOnKart):
    task_namespace = __name__
    pass


class _DummyTask(TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()
    task = gokart.TaskInstanceParameter(default=_DummySubTask())


class ListTaskInstanceParameterTest(unittest.TestCase):
    def setUp(self):
        _DummyTask.clear_instance_cache()

    def test_serialize_and_parse(self):
        original = [_DummyTask(param=3), _DummyTask(param=3)]
        s = gokart.ListTaskInstanceParameter().serialize(original)
        parsed = gokart.ListTaskInstanceParameter().parse(s)
        self.assertEqual(parsed[0].task_id, original[0].task_id)
        self.assertEqual(parsed[1].task_id, original[1].task_id)


if __name__ == '__main__':
    unittest.main()
