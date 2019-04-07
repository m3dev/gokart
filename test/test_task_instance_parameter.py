import unittest

import luigi

import gokart
from gokart import TaskOnKart, TaskInstanceParameter


class _DummySubTask(TaskOnKart):
    task_namespace = __name__
    pass


class _DummyTask(TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()
    task = TaskInstanceParameter(default=_DummySubTask())


class TaskInstanceParameterTest(unittest.TestCase):
    def setUp(self):
        _DummyTask.clear_instance_cache()

    def test_serialize_and_parse(self):
        original = _DummyTask(param=2)
        s = gokart.TaskInstanceParameter().serialize(original)
        parsed = gokart.TaskInstanceParameter().parse(s)
        self.assertEqual(parsed.task_id, original.task_id)


if __name__ == '__main__':
    unittest.main()
