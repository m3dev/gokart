import tempfile
import unittest
from collections import Counter
from typing import Any

import luigi

import gokart
from gokart.tree.task_info_formatter import RequiredTask, _make_requires_info, make_task_info_tree


class _RequiredTaskExampleTaskA(gokart.TaskOnKart[Any]):
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
        requires = {'taskA': _RequiredTaskExampleTaskA()}
        resulted = _make_requires_info(requires=requires)
        expected = {key: RequiredTask(name=require.__class__.__name__, unique_id=require.make_unique_id()) for key, require in requires.items()}
        self.assertEqual(resulted, expected)

    def test_make_requires_info_with_invalid(self):
        requires = [1, 2]
        with self.assertRaises(TypeError):
            _make_requires_info(requires=requires)


_complete_calls: Counter[str] = Counter()


class _CountingTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    name: luigi.Parameter = luigi.Parameter()
    dependencies: luigi.ListParameter = luigi.ListParameter(default=())

    def requires(self):
        return [_CountingTask(name=dep, workspace_directory=self.workspace_directory, dependencies=_DIAMOND[dep]) for dep in self.dependencies]

    def output(self):
        return self.make_target(f'{self.name}.pkl')

    def complete(self) -> bool:
        _complete_calls[self.name] += 1
        return super().complete()

    def run(self):
        self.dump(self.name)


# A -> B, C -> D (shared leaf)
_DIAMOND: dict[str, tuple[str, ...]] = {'A': ('B', 'C'), 'B': ('D',), 'C': ('D',), 'D': ()}


class TestMakeTaskInfoTree(unittest.TestCase):
    def setUp(self) -> None:
        _complete_calls.clear()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def _diamond_root(self) -> _CountingTask:
        return _CountingTask(name='A', dependencies=_DIAMOND['A'], workspace_directory=self._tmp.name)

    def test_complete_is_called_once_per_task_in_diamond_dependency(self):
        make_task_info_tree(self._diamond_root())
        self.assertEqual(dict(_complete_calls), {'A': 1, 'B': 1, 'C': 1, 'D': 1})

    def test_task_info_reflects_completion_state(self):
        root = self._diamond_root()
        _CountingTask(name='D', workspace_directory=self._tmp.name).run()
        _complete_calls.clear()

        info = make_task_info_tree(root)

        self.assertEqual(info.is_complete, 'PENDING')
        b, c = info.children_task_infos
        self.assertEqual([b.name, c.name], ['_CountingTask', '_CountingTask'])
        self.assertEqual([b.is_complete, c.is_complete], ['PENDING', 'PENDING'])
        self.assertEqual(b.children_task_infos[0].is_complete, 'COMPLETE')
        self.assertEqual(c.children_task_infos[0].is_complete, 'COMPLETE')

    def test_shared_task_is_the_same_task_info(self):
        info = make_task_info_tree(self._diamond_root())
        b, c = info.children_task_infos
        self.assertIs(b.children_task_infos[0], c.children_task_infos[0])

    def test_rerun_task_is_completed_check_once_and_pending(self):
        leaf = _CountingTask(name='D', workspace_directory=self._tmp.name)
        leaf.run()
        rerun_leaf = _CountingTask(name='D', workspace_directory=self._tmp.name, rerun=True)
        _complete_calls.clear()

        info = make_task_info_tree(rerun_leaf)

        self.assertEqual(info.is_complete, 'PENDING')
        self.assertEqual(_complete_calls['D'], 1)
        self.assertFalse(leaf.output().exists())
