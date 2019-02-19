import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import luigi

import gokart
from gokart.target import TargetOnKart, SingleFileTarget, ModelTarget


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter(default=1)
    list_param = luigi.ListParameter(default=['a', 'b'])
    bool_param = luigi.BoolParameter()


class TaskTest(unittest.TestCase):
    def setUp(self):
        _DummyTask.clear_instance_cache()

    def test_complete_without_dependency(self):
        task = _DummyTask()
        self.assertTrue(
            task.complete(), msg='_DummyTask does not have any output files, so this always must be completed.')

    def test_complete_with_rerun_flag(self):
        task = _DummyTask(rerun=True)
        self.assertFalse(task.complete(), msg='"rerun" flag force tasks rerun once.')
        self.assertTrue(task.complete(), msg='"rerun" flag should be changed.')

    def test_complete_with_uncompleted_input(self):
        uncompleted_target = MagicMock(spec=TargetOnKart)
        uncompleted_target.exists.return_value = False

        # depends on an uncompleted target.
        task = _DummyTask()
        task.input = MagicMock(return_value=uncompleted_target)
        self.assertTrue(task.complete(), msg='task does not care input targets.')

        # make a task check its inputs.
        task.strict_check = True
        self.assertFalse(task.complete())

    def test_complete_with_modified_input(self):
        input_target = MagicMock(spec=TargetOnKart)
        input_target.exists.return_value = True
        input_target.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)
        output_target = MagicMock(spec=TargetOnKart)
        output_target.exists.return_value = True
        output_target.last_modification_time.return_value = datetime(2018, 1, 1, 9, 0, 0)

        # depends on an uncompleted target.
        task = _DummyTask()
        task.modification_time_check = False
        task.input = MagicMock(return_value=input_target)
        task.output = MagicMock(return_value=output_target)
        self.assertTrue(task.complete(), msg='task does not care modified time')

        # make a task check its inputs.
        task.modification_time_check = True
        self.assertFalse(task.complete())

    def test_complete_when_modification_time_equals_output(self):
        """ Test the case that modification time of input equals that of output.
        The case is occurred when input and output targets are same.
        """
        input_target = MagicMock(spec=TargetOnKart)
        input_target.exists.return_value = True
        input_target.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)
        output_target = MagicMock(spec=TargetOnKart)
        output_target.exists.return_value = True
        output_target.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)

        task = _DummyTask()
        task.modification_time_check = True
        task.input = MagicMock(return_value=input_target)
        task.output = MagicMock(return_value=output_target)
        self.assertTrue(task.complete())

    def test_make_target(self):
        task = _DummyTask()
        target = task.make_target('test.txt')
        self.assertIsInstance(target, SingleFileTarget)

    def test_make_target_without_id(self):
        path = _DummyTask().make_target('test.txt', use_unique_id=False)._target.path
        self.assertEqual(path, os.path.join(_DummyTask().workspace_directory, 'test.txt'))

    def test_compare_targets_of_different_tasks(self):
        path1 = _DummyTask(param=1).make_target('test.txt')._target.path
        path2 = _DummyTask(param=2).make_target('test.txt')._target.path
        self.assertNotEqual(path1, path2, msg='different tasks must generate different targets.')

    def test_make_model_target(self):
        task = _DummyTask()
        target = task.make_model_target('test.zip', save_function=MagicMock(), load_function=MagicMock())
        self.assertIsInstance(target, ModelTarget)

    def test_load_with_single_target(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        target.load.return_value = 1
        task.input = MagicMock(return_value=target)

        data = task.load()
        target.load.assert_called_once()
        self.assertEqual(data, 1)

    def test_load_with_keyword(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        target.load.return_value = 1
        task.input = MagicMock(return_value={'target_key': target})

        data = task.load('target_key')
        target.load.assert_called_once()
        self.assertEqual(data, 1)

    def test_load_dictionary_at_once(self):
        task = _DummyTask()
        target1 = MagicMock(spec=TargetOnKart)
        target1.load.return_value = 1
        target2 = MagicMock(spec=TargetOnKart)
        target2.load.return_value = 2
        task.input = MagicMock(return_value={'target_key_1': target1, 'target_key_2': target2})

        data = task.load()
        target1.load.assert_called_once()
        target2.load.assert_called_once()
        self.assertEqual(data['target_key_1'], 1)
        self.assertEqual(data['target_key_2'], 2)

    def test_dump(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        task.output = MagicMock(return_value=target)

        task.dump(1)
        target.dump.assert_called_once()

    @patch('luigi.configuration.get_config')
    def test_add_configuration(self, mock_config: MagicMock):
        mock_config.return_value = {'_DummyTask': {'list_param': '["c", "d"]', 'param': '3', 'bool_param': 'True'}}
        kwargs = dict()
        _DummyTask._add_configuration(kwargs, '_DummyTask')
        self.assertEqual(3, kwargs['param'])
        self.assertEqual(['c', 'd'], list(kwargs['list_param']))
        self.assertEqual(True, kwargs['bool_param'])


if __name__ == '__main__':
    unittest.main()
