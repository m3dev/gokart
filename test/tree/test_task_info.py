import unittest
from typing import Any
from unittest.mock import patch

import luigi
import luigi.mock
from luigi.mock import MockFileSystem, MockTarget

import gokart
from gokart.tree.task_info import dump_task_info_table, dump_task_info_tree, make_task_info_as_tree_str, make_task_info_tree


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
        luigi.setup_logging.DaemonLogging._configured = False
        luigi.setup_logging.InterfaceLogging._configured = False

    def tearDown(self) -> None:
        luigi.setup_logging.DaemonLogging._configured = False
        luigi.setup_logging.InterfaceLogging._configured = False

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_pending(self):
        task = _Task(param=1, sub=_SubTask(param=2))

        # check before running
        tree = make_task_info_as_tree_str(task)
        expected = r"""
└─-\(PENDING\) _Task\[[a-z0-9]*\]
   └─-\(PENDING\) _SubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_complete(self):
        task = _Task(param=1, sub=_SubTask(param=2))

        # check after sub task runs
        gokart.build(task, reset_register=False)
        tree = make_task_info_as_tree_str(task)
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
        gokart.build(task, reset_register=False)
        tree = make_task_info_as_tree_str(task)
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
        gokart.build(task, reset_register=False)
        tree = make_task_info_as_tree_str(task, abbr=False)
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
        gokart.build(task, reset_register=False)
        tree = make_task_info_as_tree_str(task, abbr=False, ignore_task_names=['_Task'])
        expected = r"""
└─-\(COMPLETE\) _DoubleLoadSubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_with_cache(self):
        task = _DoubleLoadSubTask(
            sub1=_Task(param=1, sub=_SubTask(param=2)),
            sub2=_Task(param=1, sub=_SubTask(param=2)),
        )

        # check child task_info is the same object
        tree = make_task_info_tree(task)
        self.assertTrue(tree.children_task_infos[0] is tree.children_task_infos[1])


class _TaskInfoExampleTaskA(gokart.TaskOnKart):
    task_namespace = __name__


class _TaskInfoExampleTaskB(gokart.TaskOnKart):
    task_namespace = __name__


class _TaskInfoExampleTaskC(gokart.TaskOnKart):
    task_namespace = __name__

    def requires(self):
        return dict(taskA=_TaskInfoExampleTaskA(), taskB=_TaskInfoExampleTaskB())

    def run(self):
        self.dump('DONE')


class TestTaskInfoTable(unittest.TestCase):
    def test_dump_task_info_table(self):
        with patch('gokart.target.SingleFileTarget.dump') as mock_obj:
            self.dumped_data: Any = None

            def _side_effect(obj, lock_at_dump):
                self.dumped_data = obj

            mock_obj.side_effect = _side_effect
            dump_task_info_table(task=_TaskInfoExampleTaskC(), task_info_dump_path='path.csv', ignore_task_names=['_TaskInfoExampleTaskB'])

            self.assertEqual(set(self.dumped_data['name']), {'_TaskInfoExampleTaskA', '_TaskInfoExampleTaskC'})
            self.assertEqual(
                set(self.dumped_data.columns), {'name', 'unique_id', 'output_paths', 'params', 'processing_time', 'is_complete', 'task_log', 'requires'}
            )


class TestTaskInfoTree(unittest.TestCase):
    def test_dump_task_info_tree(self):
        with patch('gokart.target.SingleFileTarget.dump') as mock_obj:
            self.dumped_data: Any = None

            def _side_effect(obj, lock_at_dump):
                self.dumped_data = obj

            mock_obj.side_effect = _side_effect
            dump_task_info_tree(task=_TaskInfoExampleTaskC(), task_info_dump_path='path.pkl', ignore_task_names=['_TaskInfoExampleTaskB'])

            self.assertEqual(self.dumped_data.name, '_TaskInfoExampleTaskC')
            self.assertEqual(self.dumped_data.children_task_infos[0].name, '_TaskInfoExampleTaskA')

            self.assertEqual(self.dumped_data.requires.keys(), {'taskA', 'taskB'})
            self.assertEqual(self.dumped_data.requires['taskA'].name, '_TaskInfoExampleTaskA')
            self.assertEqual(self.dumped_data.requires['taskB'].name, '_TaskInfoExampleTaskB')

    def test_dump_task_info_tree_with_invalid_path_extention(self):
        with patch('gokart.target.SingleFileTarget.dump') as mock_obj:
            self.dumped_data = None

            def _side_effect(obj, lock_at_dump):
                self.dumped_data = obj

            mock_obj.side_effect = _side_effect
            with self.assertRaises(AssertionError):
                dump_task_info_tree(task=_TaskInfoExampleTaskC(), task_info_dump_path='path.csv', ignore_task_names=['_TaskInfoExampleTaskB'])
