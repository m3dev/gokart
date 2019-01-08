import unittest

import luigi

import gokart
from gokart import TaskOnKart


class _DummyTask(TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()


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
