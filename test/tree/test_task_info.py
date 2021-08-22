import unittest
from unittest.mock import patch

import luigi
import luigi.mock
from luigi.mock import MockFileSystem, MockTarget

import gokart
from gokart.tree.task_info import make_tree_info_string


class _SubTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter()

    def output(self):
        return self.make_target('sub_task.txt')

    def run(self):
        self.dump(f'task uid = {self.make_unique_id()}')


class _Task(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter(default=10)
    sub = gokart.TaskInstanceParameter(default=_SubTask(param=20))

    def requires(self):
        return self.sub

    def output(self):
        return self.make_target('task.txt')

    def run(self):
        self.dump(f'task uid = {self.make_unique_id()}')


class _DoubleLoadSubTask(gokart.TaskOnKart):
    task_namespace = __name__
    sub1 = gokart.TaskInstanceParameter()
    sub2 = gokart.TaskInstanceParameter()

    def output(self):
        return self.make_target('sub_task.txt')

    def run(self):
        self.dump(f'task uid = {self.make_unique_id()}')


class TestInfo(unittest.TestCase):
    def setUp(self) -> None:
        MockFileSystem().clear()

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_pending(self):
        task = _Task(param=1, sub=_SubTask(param=2))

        # check before running
        tree = make_tree_info_string(task)
        expected = r"""
└─-\(PENDING\) _Task\[[a-z0-9]*\]
   └─-\(PENDING\) _SubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_complete(self):
        task = _Task(param=1, sub=_SubTask(param=2))

        # check after sub task runs
        luigi.build([task], local_scheduler=True)
        tree = make_tree_info_string(task)
        expected = r"""
└─-\(COMPLETE\) _Task\[[a-z0-9]*\]
   └─-\(COMPLETE\) _SubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_abbreviation(self):
        task = _DoubleLoadSubTask(
            sub1=_Task(param=1, sub=_SubTask(param=2)),
            sub2=_Task(param=1, sub=_SubTask(param=2)),
        )

        # check after sub task runs
        luigi.build([task], local_scheduler=True)
        tree = make_tree_info_string(task)
        expected = r"""
└─-\(COMPLETE\) _DoubleLoadSubTask\[[a-z0-9]*\]
   \|--\(COMPLETE\) _Task\[[a-z0-9]*\]
   \|  └─-\(COMPLETE\) _SubTask\[[a-z0-9]*\]
   └─-\(COMPLETE\) _Task\[[a-z0-9]*\]
      └─- \.\.\.$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_not_compress(self):
        task = _DoubleLoadSubTask(
            sub1=_Task(param=1, sub=_SubTask(param=2)),
            sub2=_Task(param=1, sub=_SubTask(param=2)),
        )

        # check after sub task runs
        luigi.build([task], local_scheduler=True)
        tree = make_tree_info_string(task, abbr=False)
        expected = r"""
└─-\(COMPLETE\) _DoubleLoadSubTask\[[a-z0-9]*\]
   \|--\(COMPLETE\) _Task\[[a-z0-9]*\]
   \|  └─-\(COMPLETE\) _SubTask\[[a-z0-9]*\]
   └─-\(COMPLETE\) _Task\[[a-z0-9]*\]
      └─-\(COMPLETE\) _SubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_not_compress_ignore_task(self):
        task = _DoubleLoadSubTask(
            sub1=_Task(param=1, sub=_SubTask(param=2)),
            sub2=_Task(param=1, sub=_SubTask(param=2)),
        )

        # check after sub task runs
        luigi.build([task], local_scheduler=True)
        tree = make_tree_info_string(task, abbr=False, ignore_task_names=['_Task'])
        expected = r"""
└─-\(COMPLETE\) _DoubleLoadSubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)
