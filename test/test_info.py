import unittest
from unittest.mock import patch

import luigi
import luigi.mock
from luigi.mock import MockFileSystem, MockTarget

import gokart
import gokart.info
from test.tree.test_task_info import _DoubleLoadSubTask, _SubTask, _Task


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
        tree = gokart.info.make_tree_info(task)
        expected = r"""
└─-\(PENDING\) _Task\[[a-z0-9]*\]
   └─-\(PENDING\) _SubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)

    @patch('luigi.LocalTarget', new=lambda path, **kwargs: MockTarget(path, **kwargs))
    def test_make_tree_info_complete(self):
        task = _Task(param=1, sub=_SubTask(param=2))

        # check after sub task runs
        gokart.build(task, reset_register=False)
        tree = gokart.info.make_tree_info(task)
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
        tree = gokart.info.make_tree_info(task)
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
        tree = gokart.info.make_tree_info(task, abbr=False)
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
        tree = gokart.info.make_tree_info(task, abbr=False, ignore_task_names=['_Task'])
        expected = r"""
└─-\(COMPLETE\) _DoubleLoadSubTask\[[a-z0-9]*\]$"""
        self.assertRegex(tree, expected)


if __name__ == '__main__':
    unittest.main()
