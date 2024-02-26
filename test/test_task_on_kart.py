import os
import pathlib
import unittest
from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import luigi
import pandas as pd
from luigi.parameter import ParameterVisibility
from luigi.util import inherits

import gokart
from gokart.file_processor import XmlFileProcessor
from gokart.parameter import ListTaskInstanceParameter, TaskInstanceParameter
from gokart.target import ModelTarget, SingleFileTarget, TargetOnKart


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.IntParameter(default=1)
    list_param = luigi.ListParameter(default=['a', 'b'])
    bool_param = luigi.BoolParameter()

    def output(self):
        return None


class _DummyTaskA(gokart.TaskOnKart):
    task_namespace = __name__

    def output(self):
        return None


@inherits(_DummyTaskA)
class _DummyTaskB(gokart.TaskOnKart):
    task_namespace = __name__

    def output(self):
        return None

    def requires(self):
        return self.clone(_DummyTaskA)


@inherits(_DummyTaskB)
class _DummyTaskC(gokart.TaskOnKart):
    task_namespace = __name__

    def output(self):
        return None

    def requires(self):
        return self.clone(_DummyTaskB)


class _DummyTaskD(gokart.TaskOnKart):
    task_namespace = __name__


class _DummyTaskWithoutLock(gokart.TaskOnKart):
    task_namespace = __name__

    def run(self):
        pass


class _DummySubTaskWithPrivateParameter(gokart.TaskOnKart):
    task_namespace = __name__


class _DummyTaskWithPrivateParameter(gokart.TaskOnKart):
    task_namespace = __name__
    int_param = luigi.IntParameter()
    private_int_param = luigi.IntParameter(visibility=ParameterVisibility.PRIVATE)
    task_param = TaskInstanceParameter()
    list_task_param = ListTaskInstanceParameter()


class TaskTest(unittest.TestCase):
    def setUp(self):
        _DummyTask.clear_instance_cache()
        _DummyTaskA.clear_instance_cache()
        _DummyTaskB.clear_instance_cache()
        _DummyTaskC.clear_instance_cache()

    def test_complete_without_dependency(self):
        task = _DummyTask()
        self.assertTrue(task.complete(), msg='_DummyTask does not have any output files, so this always must be completed.')

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
        """Test the case that modification time of input equals that of output.
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

    def test_complete_when_input_and_output_equal(self):
        target1 = MagicMock(spec=TargetOnKart)
        target1.exists.return_value = True
        target1.path.return_value = 'path1.pkl'
        target1.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)

        target2 = MagicMock(spec=TargetOnKart)
        target2.exists.return_value = True
        target2.path.return_value = 'path2.pkl'
        target2.last_modification_time.return_value = datetime(2018, 1, 1, 9, 0, 0)

        target3 = MagicMock(spec=TargetOnKart)
        target3.exists.return_value = True
        target3.path.return_value = 'path3.pkl'
        target3.last_modification_time.return_value = datetime(2018, 1, 1, 9, 0, 0)

        task = _DummyTask()
        task.modification_time_check = True
        task.input = MagicMock(return_value=[target1, target2])
        task.output = MagicMock(return_value=[target1, target2])
        self.assertTrue(task.complete())

        task.input = MagicMock(return_value=[target1, target2])
        task.output = MagicMock(return_value=[target2, target3])
        self.assertFalse(task.complete())

    def test_default_target(self):
        task = _DummyTaskD()
        default_target = task.output()
        self.assertIsInstance(default_target, SingleFileTarget)
        self.assertEqual(f'_DummyTaskD_{task.task_unique_id}.pkl', pathlib.Path(default_target._target.path).name)

    def test_clone_with_special_params(self):
        class _DummyTaskRerun(gokart.TaskOnKart):
            a = luigi.BoolParameter(default=False)

        task = _DummyTaskRerun(a=True, rerun=True)
        cloned = task.clone(_DummyTaskRerun)
        cloned_with_explicit_rerun = task.clone(_DummyTaskRerun, rerun=True)
        self.assertTrue(cloned.a)
        self.assertFalse(cloned.rerun)  # do not clone rerun
        self.assertTrue(cloned_with_explicit_rerun.a)
        self.assertTrue(cloned_with_explicit_rerun.rerun)

    def test_default_large_dataframe_target(self):
        task = _DummyTaskD()
        default_large_dataframe_target = task.make_large_data_frame_target()
        self.assertIsInstance(default_large_dataframe_target, ModelTarget)
        self.assertEqual(f'_DummyTaskD_{task.task_unique_id}.zip', pathlib.Path(default_large_dataframe_target._zip_client._file_path).name)

    def test_make_target(self):
        task = _DummyTask()
        target = task.make_target('test.txt')
        self.assertIsInstance(target, SingleFileTarget)

    def test_make_target_without_id(self):
        path = _DummyTask().make_target('test.txt', use_unique_id=False)._target.path
        self.assertEqual(path, os.path.join(_DummyTask().workspace_directory, 'test.txt'))

    def test_make_target_with_processor(self):
        task = _DummyTask()
        processor = XmlFileProcessor()
        target = task.make_target('test.dummy', processor=processor)
        self.assertEqual(target._processor, processor)
        self.assertIsInstance(target, SingleFileTarget)

    def test_get_own_code(self):
        task = _DummyTask()
        task_scripts = 'def output(self):\nreturn None\n'
        self.assertEqual(task.get_own_code().replace(' ', ''), task_scripts.replace(' ', ''))

    def test_make_unique_id_with_own_code(self):
        class _MyDummyTaskA(gokart.TaskOnKart):
            _visible_in_registry = False

            def run(self):
                self.dump('Hello, world!')

        task_unique_id = _MyDummyTaskA(serialized_task_definition_check=False).make_unique_id()
        task_with_code_unique_id = _MyDummyTaskA(serialized_task_definition_check=True).make_unique_id()
        self.assertNotEqual(task_unique_id, task_with_code_unique_id)

        class _MyDummyTaskA(gokart.TaskOnKart):
            _visible_in_registry = False

            def run(self):
                modified_code = 'modify!!'
                self.dump(modified_code)

        task_modified_unique_id = _MyDummyTaskA(serialized_task_definition_check=False).make_unique_id()
        task_modified_with_code_unique_id = _MyDummyTaskA(serialized_task_definition_check=True).make_unique_id()
        self.assertEqual(task_modified_unique_id, task_unique_id)
        self.assertNotEqual(task_modified_with_code_unique_id, task_with_code_unique_id)

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

    def test_load_with_single_dict_target(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        target.load.return_value = 1
        task.input = MagicMock(return_value={'target_key': target})

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

    def test_load_tuple(self):
        task = _DummyTask()
        target1 = MagicMock(spec=TargetOnKart)
        target1.load.return_value = 1
        target2 = MagicMock(spec=TargetOnKart)
        target2.load.return_value = 2
        task.input = MagicMock(return_value=(target1, target2))

        data = task.load()
        target1.load.assert_called_once()
        target2.load.assert_called_once()
        self.assertEqual(data[0], 1)
        self.assertEqual(data[1], 2)

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

    def test_load_generator_with_single_target(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        target.load.return_value = [1, 2]
        task.input = MagicMock(return_value=target)
        data = [x for x in task.load_generator()]
        self.assertEqual(data, [[1, 2]])

    def test_load_generator_with_keyword(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        target.load.return_value = [1, 2]
        task.input = MagicMock(return_value={'target_key': target})
        data = [x for x in task.load_generator('target_key')]
        self.assertEqual(data, [[1, 2]])

    def test_dump(self):
        task = _DummyTask()
        target = MagicMock(spec=TargetOnKart)
        task.output = MagicMock(return_value=target)

        task.dump(1)
        target.dump.assert_called_once()

    def test_fail_on_empty_dump(self):
        # do not fail
        task = _DummyTask(fail_on_empty_dump=False)
        target = MagicMock(spec=TargetOnKart)
        task.output = MagicMock(return_value=target)
        task.dump(pd.DataFrame())
        target.dump.assert_called_once()

        # fail
        task = _DummyTask(fail_on_empty_dump=True)
        self.assertRaises(AssertionError, lambda: task.dump(pd.DataFrame()))

    @patch('luigi.configuration.get_config')
    def test_add_configuration(self, mock_config: MagicMock):
        mock_config.return_value = {'_DummyTask': {'list_param': '["c", "d"]', 'param': '3', 'bool_param': 'True'}}
        kwargs: Dict[str, Any] = dict()
        _DummyTask._add_configuration(kwargs, '_DummyTask')
        self.assertEqual(3, kwargs['param'])
        self.assertEqual(['c', 'd'], list(kwargs['list_param']))
        self.assertEqual(True, kwargs['bool_param'])

    @patch('luigi.cmdline_parser.CmdlineParser.get_instance')
    def test_add_cofigureation_evaluation_order(self, mock_cmdline: MagicMock):
        """
        in case TaskOnKart._add_configuration will break evaluation order
        @see https://luigi.readthedocs.io/en/stable/parameters.html#parameter-resolution-order
        """

        class DummyTaskAddConfiguration(gokart.TaskOnKart):
            aa = luigi.IntParameter()

        luigi.configuration.get_config().set('DummyTaskAddConfiguration', 'aa', '3')
        mock_cmdline.return_value = luigi.cmdline_parser.CmdlineParser(['DummyTaskAddConfiguration'])
        self.assertEqual(DummyTaskAddConfiguration().aa, 3)

        mock_cmdline.return_value = luigi.cmdline_parser.CmdlineParser(['DummyTaskAddConfiguration', '--DummyTaskAddConfiguration-aa', '2'])
        self.assertEqual(DummyTaskAddConfiguration().aa, 2)

    def test_load_list_of_list_pandas(self):
        task = _DummyTask()
        task.load = MagicMock(return_value=[pd.DataFrame(dict(a=[1])), [pd.DataFrame(dict(a=[2])), pd.DataFrame(dict(a=[3]))]])

        df = task.load_data_frame()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(3, df.shape[0])

    def test_load_single_value_dict_of_dataframe(self):
        task = _DummyTask()
        task.load = MagicMock(return_value={'a': pd.DataFrame(dict(a=[1]))})

        df = task.load_data_frame()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(1, df.shape[0])

    def test_load_data_frame_drop_columns(self):
        task = _DummyTask()
        task.load = MagicMock(return_value=pd.DataFrame(dict(a=[1], b=[2], c=[3])))

        df = task.load_data_frame(required_columns={'a', 'c'}, drop_columns=True)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(1, df.shape[0])
        self.assertSetEqual({'a', 'c'}, set(df.columns))

    def test_load_data_frame_empty_input(self):
        task = _DummyTask()
        task.load = MagicMock(return_value=pd.DataFrame(dict(a=[], b=[], c=[])))

        df = task.load_data_frame(required_columns={'a', 'c'})
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(0, df.shape[0])
        self.assertSetEqual({'a', 'b', 'c'}, set(df.columns))

    def test_load_index_only_dataframe(self):
        task = _DummyTask()
        task.load = MagicMock(return_value=pd.DataFrame(index=range(3)))

        # connnot load index only frame with required_columns
        self.assertRaises(AssertionError, lambda: task.load_data_frame(required_columns={'a', 'c'}))

        df: pd.DataFrame = task.load_data_frame()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)
        self.assertListEqual(list(range(3)), list(df.index))

    def test_use_rerun_with_inherits(self):
        # All tasks are completed.
        task_c = _DummyTaskC()
        self.assertTrue(task_c.complete())
        self.assertTrue(task_c.requires().complete())  # This is an instance of TaskB.
        self.assertTrue(task_c.requires().requires().complete())  # This is an instance of TaskA.

        luigi.configuration.get_config().set(f'{__name__}._DummyTaskB', 'rerun', 'True')
        task_c = _DummyTaskC()
        self.assertTrue(task_c.complete())
        self.assertFalse(task_c.requires().complete())  # This is an instance of _DummyTaskB.
        self.assertTrue(task_c.requires().requires().complete())  # This is an instance of _DummyTaskA.

        # All tasks are not completed, because _DummyTaskC.rerun = True.
        task_c = _DummyTaskC(rerun=True)
        self.assertFalse(task_c.complete())
        self.assertTrue(task_c.requires().complete())  # This is an instance of _DummyTaskB.
        self.assertTrue(task_c.requires().requires().complete())  # This is an instance of _DummyTaskA.

    def test_significant_flag(self):
        def _make_task(significant: bool, has_required_task: bool):
            class _MyDummyTaskA(gokart.TaskOnKart):
                task_namespace = f'{__name__}_{significant}_{has_required_task}'

            class _MyDummyTaskB(gokart.TaskOnKart):
                task_namespace = f'{__name__}_{significant}_{has_required_task}'

                def requires(self):
                    if has_required_task:
                        return _MyDummyTaskA(significant=significant)
                    return

            return _MyDummyTaskB()

        x_task = _make_task(significant=True, has_required_task=True)
        y_task = _make_task(significant=False, has_required_task=True)
        z_task = _make_task(significant=False, has_required_task=False)

        self.assertNotEqual(x_task.make_unique_id(), y_task.make_unique_id())
        self.assertEqual(y_task.make_unique_id(), z_task.make_unique_id())

    def test_default_requires(self):
        class _WithoutTaskInstanceParameter(gokart.TaskOnKart):
            task_namespace = __name__

        class _WithTaskInstanceParameter(gokart.TaskOnKart):
            task_namespace = __name__
            a_task = gokart.TaskInstanceParameter()

        without_task = _WithoutTaskInstanceParameter()
        self.assertListEqual(without_task.requires(), [])

        with_task = _WithTaskInstanceParameter(a_task=without_task)
        self.assertEqual(with_task.requires()['a_task'], without_task)

    def test_repr(self):
        task = _DummyTaskWithPrivateParameter(
            int_param=1,
            private_int_param=1,
            task_param=_DummySubTaskWithPrivateParameter(),
            list_task_param=[_DummySubTaskWithPrivateParameter(), _DummySubTaskWithPrivateParameter()],
        )
        sub_task_id = _DummySubTaskWithPrivateParameter().make_unique_id()
        expected = (
            f'{__name__}._DummyTaskWithPrivateParameter(int_param=1, private_int_param=1, task_param={__name__}._DummySubTaskWithPrivateParameter({sub_task_id}), '
            f'list_task_param=[{__name__}._DummySubTaskWithPrivateParameter({sub_task_id}), {__name__}._DummySubTaskWithPrivateParameter({sub_task_id})])'
        )  # noqa:E501
        self.assertEqual(expected, repr(task))

    def test_str(self):
        task = _DummyTaskWithPrivateParameter(
            int_param=1,
            private_int_param=1,
            task_param=_DummySubTaskWithPrivateParameter(),
            list_task_param=[_DummySubTaskWithPrivateParameter(), _DummySubTaskWithPrivateParameter()],
        )
        sub_task_id = _DummySubTaskWithPrivateParameter().make_unique_id()
        expected = (
            f'{__name__}._DummyTaskWithPrivateParameter(int_param=1, task_param={__name__}._DummySubTaskWithPrivateParameter({sub_task_id}), '
            f'list_task_param=[{__name__}._DummySubTaskWithPrivateParameter({sub_task_id}), {__name__}._DummySubTaskWithPrivateParameter({sub_task_id})])'
        )
        self.assertEqual(expected, str(task))

    def test_is_task_on_kart(self):
        self.assertEqual(True, gokart.TaskOnKart.is_task_on_kart(gokart.TaskOnKart()))
        self.assertEqual(False, gokart.TaskOnKart.is_task_on_kart(1))
        self.assertEqual(False, gokart.TaskOnKart.is_task_on_kart(list()))
        self.assertEqual(True, gokart.TaskOnKart.is_task_on_kart((gokart.TaskOnKart(), gokart.TaskOnKart())))

    def test_serialize_and_deserialize_default_values(self):
        task = gokart.TaskOnKart()
        deserialized: gokart.TaskOnKart = luigi.task_register.load_task(None, task.get_task_family(), task.to_str_params())
        self.assertDictEqual(task.to_str_params(), deserialized.to_str_params())


class _DummyTaskWithNonCompleted(gokart.TaskOnKart):
    def dump(self, obj):
        # overrive dump() to do nothing.
        pass

    def run(self):
        self.dump('hello')

    def complete(self):
        return False


class _DummyTaskWithCompleted(gokart.TaskOnKart):
    def dump(self, obj):
        # overrive dump() to do nothing.
        pass

    def run(self):
        self.dump('hello')

    def complete(self):
        return True


class TestCompleteCheckAtRun(unittest.TestCase):
    def test_run_when_complete_check_at_run_is_false_and_task_is_not_completed(self):
        task = _DummyTaskWithNonCompleted(complete_check_at_run=False)
        task.dump = MagicMock()
        task.run()

        # since run() is called, dump() should be called.
        task.dump.assert_called_once()

    def test_run_when_complete_check_at_run_is_false_and_task_is_completed(self):
        task = _DummyTaskWithCompleted(complete_check_at_run=False)
        task.dump = MagicMock()
        task.run()

        # even task is completed, since run() is called, dump() should be called.
        task.dump.assert_called_once()

    def test_run_when_complete_check_at_run_is_true_and_task_is_not_completed(self):
        task = _DummyTaskWithNonCompleted(complete_check_at_run=True)
        task.dump = MagicMock()
        task.run()

        # since task is not completed, when run() is called, dump() should be called.
        task.dump.assert_called_once()

    def test_run_when_complete_check_at_run_is_true_and_task_is_completed(self):
        task = _DummyTaskWithCompleted(complete_check_at_run=True)
        task.dump = MagicMock()
        task.run()

        # since task is completed, even when run() is called, dump() should not be called.
        task.dump.assert_not_called()


if __name__ == '__main__':
    unittest.main()
