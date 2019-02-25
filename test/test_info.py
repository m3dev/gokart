import unittest
from unittest.mock import patch, MagicMock

import luigi
import luigi.mock
import gokart
import gokart.info


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


class TestInfo(unittest.TestCase):
    @patch('luigi.LocalTarget', new=lambda path, **kwargs: luigi.mock.MockTarget(path, **kwargs))
    def test_make_tree_info(self):
        task = _Task(param=1, sub=_SubTask(param=2))

        # check before running
        tree = gokart.info.make_tree_info(task)
        expected = r"""
└─-\(PENDING\) _Task\[[a-z0-9]*\]
   └─-\(PENDING\) _SubTask\[[a-z0-9]*\]"""
        self.assertRegex(tree, expected)

        # check after sub task runs
        luigi.build([task], local_scheduler=True, log_level='CRITICAL')
        tree = gokart.info.make_tree_info(task)
        expected = r"""
└─-\(COMPLETE\) _Task\[[a-z0-9]*\]
   └─-\(COMPLETE\) _SubTask\[[a-z0-9]*\]"""
        self.assertRegex(tree, expected)


if __name__ == '__main__':
    unittest.main()
