import unittest

import gokart
from gokart.tree.task_info_formatter import RequiredTask, _make_requires_info


class _RequiredTaskExampleTaskA(gokart.TaskOnKart):
    task_namespace = __name__


class TestMakeRequiresInfo(unittest.TestCase):
    def test_make_requires_info_with_task_on_kart(self):
        requires = _RequiredTaskExampleTaskA()
        resulted = _make_requires_info(requires=requires)
        expected = RequiredTask(name=requires.__class__.__name__, unique_id=requires.make_unique_id())
        self.assertEqual(resulted, expected)

    def test_make_requires_info_with_list(self):
        requires = [_RequiredTaskExampleTaskA()]
        resulted = _make_requires_info(requires=requires)
        expected = [RequiredTask(name=require.__class__.__name__, unique_id=require.make_unique_id()) for require in requires]
        self.assertEqual(resulted, expected)

    def test_make_requires_info_with_generator(self):
        def _requires_gen():
            return (_RequiredTaskExampleTaskA() for _ in range(2))

        resulted = _make_requires_info(requires=_requires_gen())
        expected = [RequiredTask(name=require.__class__.__name__, unique_id=require.make_unique_id()) for require in _requires_gen()]
        self.assertEqual(resulted, expected)

    def test_make_requires_info_with_dict(self):
        requires = dict(taskA=_RequiredTaskExampleTaskA())
        resulted = _make_requires_info(requires=requires)
        expected = {key: RequiredTask(name=require.__class__.__name__, unique_id=require.make_unique_id()) for key, require in requires.items()}
        self.assertEqual(resulted, expected)

    def test_make_requires_info_with_invalid(self):
        requires = [1, 2]
        with self.assertRaises(TypeError):
            _make_requires_info(requires=requires)
