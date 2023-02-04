import unittest

import luigi

import gokart
from gokart import ListTaskInstanceParameter, TaskInstanceParameter, TaskOnKart


class _DummySubTask(TaskOnKart):
    task_namespace = __name__
    pass


class _DummyCorrectSubClassTask(_DummySubTask):
    task_namespace = __name__
    pass


class _DummyInvalidSubClassTask(TaskOnKart):
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

    def test_invalid_bound(self):
        self.assertRaises(ValueError, lambda: gokart.TaskInstanceParameter(bound=1))  # not type instance

    def test_params_with_correct_subclass_bound(self):

        class _DummyPipelineA(TaskOnKart):
            task_namespace = __name__
            subtask = gokart.TaskInstanceParameter(bound=_DummySubTask)

        task = _DummyPipelineA(subtask=_DummyCorrectSubClassTask())
        self.assertEqual(task.requires()['subtask'], _DummyCorrectSubClassTask())

    def test_params_with_invalid_subclass_bound(self):

        class _DummyPipelineB(TaskOnKart):
            task_namespace = __name__
            subtask = gokart.TaskInstanceParameter(bound=_DummySubTask)

        with self.assertRaises(ValueError):
            _DummyPipelineB(subtask=_DummyInvalidSubClassTask())


class ListTaskInstanceParameterTest(unittest.TestCase):

    def setUp(self):
        _DummyTask.clear_instance_cache()

    def test_invalid_bound(self):
        self.assertRaises(ValueError, lambda: gokart.ListTaskInstanceParameter(bound=1))  # not type instance

    def test_list_params_with_correct_subclass_bound(self):

        class _DummyPipelineC(TaskOnKart):
            task_namespace = __name__
            subtask = gokart.ListTaskInstanceParameter(bound=_DummySubTask)

        task = _DummyPipelineC(subtask=[_DummyCorrectSubClassTask()])
        self.assertEqual(task.requires()['subtask'], (_DummyCorrectSubClassTask(),))

    def test_list_params_with_invalid_subclass_bound(self):

        class _DummyPipelineD(TaskOnKart):
            task_namespace = __name__
            subtask = gokart.ListTaskInstanceParameter(bound=_DummySubTask)

        with self.assertRaises(ValueError):
            _DummyPipelineD(subtask=[_DummyInvalidSubClassTask(), _DummyCorrectSubClassTask])


if __name__ == '__main__':
    unittest.main()
