import unittest
from unittest.mock import patch

import pandas as pd

import gokart
from gokart.task_info import _make_task_info_table, dump_task_info_table


class _TaskInfoExampleTaskA(gokart.TaskOnKart):
    pass


class _TaskInfoExampleTaskB(gokart.TaskOnKart):
    pass


class _TaskInfoExampleTaskC(gokart.TaskOnKart):
    def requires(self):
        return dict(taskA=_TaskInfoExampleTaskA(), taskB=_TaskInfoExampleTaskB())

    def run(self):
        self.dump('DONE')


class TestTaskInfo(unittest.TestCase):
    def test_make_task_info_table(self):
        resulted = _make_task_info_table(_TaskInfoExampleTaskC(), ignore_task_names=['_TaskInfoExampleTaskB'])
        self.assertEqual(set(resulted['name']), {'_TaskInfoExampleTaskA', '_TaskInfoExampleTaskC'})

    def test_dump_task_info_table(self):
        with patch('gokart.target.SingleFileTarget.dump') as mock_obj:
            self.dumped_data = None

            def _side_effect(obj, lock_at_dump):
                self.dumped_data = obj

            mock_obj.side_effect = _side_effect
            dump_task_info_table(task=_TaskInfoExampleTaskC(), task_info_dump_path='path.csv', task_info_ignore_task_names=['_TaskInfoExampleTaskB'])

            self.assertEqual(set(self.dumped_data['name']), {'_TaskInfoExampleTaskA', '_TaskInfoExampleTaskC'})

    def test_dump_task_info_table_with_no_path(self):
        with patch('gokart.target.SingleFileTarget.dump') as mock_obj:
            self.dumped_data = None

            def _side_effect(obj, lock_at_dump):
                self.dumped_data = obj

            mock_obj.side_effect = _side_effect
            dump_task_info_table(task=_TaskInfoExampleTaskC(), task_info_dump_path=None, task_info_ignore_task_names=['_TaskInfoExampleTaskB'])

            self.assertEqual(self.dumped_data, None)
