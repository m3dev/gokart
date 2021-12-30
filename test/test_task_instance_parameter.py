import unittest

import luigi

import gokart
from gokart import ListTaskInstanceParameter, TaskInstanceParameter, TaskOnKart


class _DummySubTask(TaskOnKart):
    task_namespace = __name__
    pass


class _DummyTask(TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()
    task = TaskInstanceParameter(default=_DummySubTask())


class _DummyListTask(TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()
    task = ListTaskInstanceParameter(default=[_DummySubTask(), _DummySubTask()])


class TaskInstanceParameterTest(unittest.TestCase):

    def setUp(self):
        _DummyTask.clear_instance_cache()

    def test_serialize_and_parse(self):
        original = _DummyTask(param=2)
        s = gokart.TaskInstanceParameter().serialize(original)
        parsed = gokart.TaskInstanceParameter().parse(s)
        self.assertEqual(parsed.task_id, original.task_id)

    def test_serialize_and_parse_list_params(self):
        original = _DummyListTask(param=2)
        s = gokart.TaskInstanceParameter().serialize(original)
        parsed = gokart.TaskInstanceParameter().parse(s)
        self.assertEqual(parsed.task_id, original.task_id)


if __name__ == '__main__':
    unittest.main()
