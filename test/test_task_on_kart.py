import os
import pathlib
import unittest
from datetime import datetime
from typing import Any, Dict, List, cast
from unittest.mock import Mock, patch

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
        uncompleted_target = Mock(spec=TargetOnKart)
        uncompleted_target.exists.return_value = False

        # depends on an uncompleted target.
        task = _DummyTask()
        task.input = Mock(return_value=uncompleted_target)  # type: ignore
        self.assertTrue(task.complete(), msg='task does not care input targets.')

        # make a task check its inputs.
        task.strict_check = True
        self.assertFalse(task.complete())

    def test_complete_with_modified_input(self):
        input_target = Mock(spec=TargetOnKart)
        input_target.exists.return_value = True
        input_target.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)
        output_target = Mock(spec=TargetOnKart)
        output_target.exists.return_value = True
        output_target.last_modification_time.return_value = datetime(2018, 1, 1, 9, 0, 0)

        # depends on an uncompleted target.
        task = _DummyTask()
        task.modification_time_check = False
        task.input = Mock(return_value=input_target)  # type: ignore
        task.output = Mock(return_value=output_target)  # type: ignore
        self.assertTrue(task.complete(), msg='task does not care modified time')

        # make a task check its inputs.
        task.modification_time_check = True
        self.assertFalse(task.complete())

    def test_complete_when_modification_time_equals_output(self):
        """Test the case that modification time of input equals that of output.
        The case is occurred when input and output targets are same.
        """
        input_target = Mock(spec=TargetOnKart)
        input_target.exists.return_value = True
        input_target.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)
        output_target = Mock(spec=TargetOnKart)
        output_target.exists.return_value = True
        output_target.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)

        task = _DummyTask()
        task.modification_time_check = True
        task.input = Mock(return_value=input_target)  # type: ignore
        task.output = Mock(return_value=output_target)  # type: ignore
        self.assertTrue(task.complete())

    def test_complete_when_input_and_output_equal(self):
        target1 = Mock(spec=TargetOnKart)
        target1.exists.return_value = True
        target1.path.return_value = 'path1.pkl'
        target1.last_modification_time.return_value = datetime(2018, 1, 1, 10, 0, 0)

        target2 = Mock(spec=TargetOnKart)
        target2.exists.return_value = True
        target2.path.return_value = 'path2.pkl'
        target2.last_modification_time.return_value = datetime(2018, 1, 1, 9, 0, 0)

        target3 = Mock(spec=TargetOnKart)
        target3.exists.return_value = True
        target3.path.return_value = 'path3.pkl'
        target3.last_modification_time.return_value = datetime(2018, 1, 1, 9, 0, 0)

        task = _DummyTask()
        task.modification_time_check = True
        task.input = Mock(return_value=[target1, target2])  # type: ignore
        task.output = Mock(return_value=[target1, target2])  # type: ignore
        self.assertTrue(task.complete())

        task.input = Mock(return_value=[target1, target2])  # type: ignore
        task.output = Mock(return_value=[target2, target3])  # type: ignore
        self.assertFalse(task.complete())

    def test_default_target(self):
        task = _DummyTaskD()
        default_target = task.output()
        self.assertIsInstance(default_target, SingleFileTarget)
        self.assertEqual(f'_DummyTaskD_{task.task_unique_id}.pkl', pathlib.Path(default_target._target.path).name)  # type: ignore

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
        target = cast(ModelTarget, default_large_dataframe_target)
        self.assertEqual(f'_DummyTaskD_{task.task_unique_id}.zip', pathlib.Path(target._zip_client.path).name)

    def test_make_target(self):
        task = _DummyTask()
        target = task.make_target('test.txt')
        self.assertIsInstance(target, SingleFileTarget)

    def test_make_target_without_id(self):
        path = _DummyTask().make_target('test.txt', use_unique_id=False).path()
        self.assertEqual(path, os.path.join(_DummyTask().workspace_directory, 'test.txt'))

    def test_make_target_with_processor(self):
        task = _DummyTask()
        processor = XmlFileProcessor()
        target = task.make_target('test.dummy', processor=processor)
        self.assertIsInstance(target, SingleFileTarget)
        target = cast(SingleFileTarget, target)
        self.assertEqual(target._processor, processor)

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

        class _MyDummyTaskA(gokart.TaskOnKart):  # type: ignore
            _visible_in_registry = False

            def run(self):
                modified_code = 'modify!!'
                self.dump(modified_code)

        task_modified_unique_id = _MyDummyTaskA(serialized_task_definition_check=False).make_unique_id()
        task_modified_with_code_unique_id = _MyDummyTaskA(serialized_task_definition_check=True).make_unique_id()
        self.assertEqual(task_modified_unique_id, task_unique_id)
        self.assertNotEqual(task_modified_with_code_unique_id, task_with_code_unique_id)

    def test_compare_targets_of_different_tasks(self):
        path1 = _DummyTask(param=1).make_target('test.txt').path()
        path2 = _DummyTask(param=2).make_target('test.txt').path()
        self.assertNotEqual(path1, path2, msg='different tasks must generate different targets.')

    def test_make_model_target(self):
        task = _DummyTask()
        target = task.make_model_target('test.zip', save_function=Mock(), load_function=Mock())
        self.assertIsInstance(target, ModelTarget)

    def test_load_with_single_target(self):
        task = _DummyTask()
        target = Mock(spec=TargetOnKart)
        target.load.return_value = 1
        task.input = Mock(return_value=target)  # type: ignore

        data = task.load()
        target.load.assert_called_once()
        self.assertEqual(data, 1)

    def test_load_with_single_dict_target(self):
        task = _DummyTask()
        target = Mock(spec=TargetOnKart)
        target.load.return_value = 1
        task.input = Mock(return_value={'target_key': target})  # type: ignore

        data = task.load()
        target.load.assert_called_once()
        self.assertEqual(data, {'target_key': 1})

    def test_load_with_keyword(self):
        task = _DummyTask()
        target = Mock(spec=TargetOnKart)
        target.load.return_value = 1
        task.input = Mock(return_value={'target_key': target})  # type: ignore

        data = task.load('target_key')
        target.load.assert_called_once()
        self.assertEqual(data, 1)

    def test_load_tuple(self):
        task = _DummyTask()
        target1 = Mock(spec=TargetOnKart)
        target1.load.return_value = 1
        target2 = Mock(spec=TargetOnKart)
        target2.load.return_value = 2
        task.input = Mock(return_value=(target1, target2))  # type: ignore

        data = task.load()
        target1.load.assert_called_once()
        target2.load.assert_called_once()
        self.assertEqual(data[0], 1)
        self.assertEqual(data[1], 2)

    def test_load_dictionary_at_once(self):
        task = _DummyTask()
        target1 = Mock(spec=TargetOnKart)
        target1.load.return_value = 1
        target2 = Mock(spec=TargetOnKart)
        target2.load.return_value = 2
        task.input = Mock(return_value={'target_key_1': target1, 'target_key_2': target2})  # type: ignore

        data = task.load()
        target1.load.assert_called_once()
        target2.load.assert_called_once()
        self.assertEqual(data['target_key_1'], 1)
        self.assertEqual(data['target_key_2'], 2)

    def test_load_with_task_on_kart(self):
        task = _DummyTask()

        task2 = Mock(spec=gokart.TaskOnKart)
        task2.make_unique_id.return_value = 'task2'
        task2_output = Mock(spec=TargetOnKart)
        task2.output.return_value = task2_output
        task2_output.load.return_value = 1

        # task2 should be in requires' return values
        task.requires = lambda: {'task2': task2}  # type: ignore

        actual = task.load(task2)
        self.assertEqual(actual, 1)

    def test_load_with_task_on_kart_should_fail_when_task_on_kart_is_not_in_requires(self):
        """
        if load args is not in requires, it should raise an error.
        """
        task = _DummyTask()

        task2 = Mock(spec=gokart.TaskOnKart)
        task2_output = Mock(spec=TargetOnKart)
        task2.output.return_value = task2_output
        task2_output.load.return_value = 1

        with self.assertRaises(AssertionError):
            task.load(task2)

    def test_load_with_task_on_kart_list(self):
        task = _DummyTask()

        task2 = Mock(spec=gokart.TaskOnKart)
        task2.make_unique_id.return_value = 'task2'
        task2_output = Mock(spec=TargetOnKart)
        task2.output.return_value = task2_output
        task2_output.load.return_value = 1

        task3 = Mock(spec=gokart.TaskOnKart)
        task3.make_unique_id.return_value = 'task3'
        task3_output = Mock(spec=TargetOnKart)
        task3.output.return_value = task3_output
        task3_output.load.return_value = 2

        # task2 should be in requires' return values
        task.requires = lambda: {'tasks': [task2, task3]}  # type: ignore

        load_args: List[gokart.TaskOnKart[int]] = [task2, task3]
        actual = task.load(load_args)
        self.assertEqual(actual, [1, 2])

    def test_load_generator_with_single_target(self):
        task = _DummyTask()
        target = Mock(spec=TargetOnKart)
        target.load.return_value = [1, 2]
        task.input = Mock(return_value=target)  # type: ignore
        data = [x for x in task.load_generator()]
        self.assertEqual(data, [[1, 2]])

    def test_load_generator_with_keyword(self):
        task = _DummyTask()
        target = Mock(spec=TargetOnKart)
        target.load.return_value = [1, 2]
        task.input = Mock(return_value={'target_key': target})  # type: ignore
        data = [x for x in task.load_generator('target_key')]
        self.assertEqual(data, [[1, 2]])

    def test_load_generator_with_list_task_on_kart(self):
        task = _DummyTask()

        task2 = Mock(spec=gokart.TaskOnKart)
        task2.make_unique_id.return_value = 'task2'
        task2_output = Mock(spec=TargetOnKart)
        task2.output.return_value = task2_output
        task2_output.load.return_value = 1

        task3 = Mock(spec=gokart.TaskOnKart)
        task3.make_unique_id.return_value = 'task3'
        task3_output = Mock(spec=TargetOnKart)
        task3.output.return_value = task3_output
        task3_output.load.return_value = 2

        # task2 should be in requires' return values
        task.requires = lambda: {'tasks': [task2, task3]}  # type: ignore

        load_args: List[gokart.TaskOnKart[int]] = [task2, task3]
        actual = [x for x in task.load_generator(load_args)]
        self.assertEqual(actual, [1, 2])

    def test_dump(self):
        task = _DummyTask()
        target = Mock(spec=TargetOnKart)
        task.output = Mock(return_value=target)  # type: ignore

        task.dump(1)  # type: ignore
        target.dump.assert_called_once()

    def test_fail_on_empty_dump(self):
        # do not fail
        task = _DummyTask(fail_on_empty_dump=False)
        target = Mock(spec=TargetOnKart)
        task.output = Mock(return_value=target)  # type: ignore
        task.dump(pd.DataFrame())
        target.dump.assert_called_once()

        # fail
        task = _DummyTask(fail_on_empty_dump=True)
        self.assertRaises(AssertionError, lambda: task.dump(pd.DataFrame()))

    @patch('luigi.configuration.get_config')
    def test_add_configuration(self, mock_config: Mock):
        mock_config.return_value = {'_DummyTask': {'list_param': '["c", "d"]', 'param': '3', 'bool_param': 'True'}}
        kwargs: Dict[str, Any] = dict()
        _DummyTask._add_configuration(kwargs, '_DummyTask')
        self.assertEqual(3, kwargs['param'])
        self.assertEqual(['c', 'd'], list(kwargs['list_param']))
        self.assertEqual(True, kwargs['bool_param'])

    @patch('luigi.cmdline_parser.CmdlineParser.get_instance')
    def test_add_cofigureation_evaluation_order(self, mock_cmdline: Mock):
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
        task.load = Mock(return_value=[pd.DataFrame(dict(a=[1])), [pd.DataFrame(dict(a=[2])), pd.DataFrame(dict(a=[3]))]])  # type: ignore

        df = task.load_data_frame()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(3, df.shape[0])

    def test_load_single_value_dict_of_dataframe(self):
        task = _DummyTask()
        task.load = Mock(return_value={'a': pd.DataFrame(dict(a=[1]))})  # type: ignore

        df = task.load_data_frame()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(1, df.shape[0])

    def test_load_data_frame_drop_columns(self):
        task = _DummyTask()
        task.load = Mock(return_value=pd.DataFrame(dict(a=[1], b=[2], c=[3])))  # type: ignore

        df = task.load_data_frame(required_columns={'a', 'c'}, drop_columns=True)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(1, df.shape[0])
        self.assertSetEqual({'a', 'c'}, set(df.columns))

    def test_load_data_frame_empty_input(self):
        task = _DummyTask()
        task.load = Mock(return_value=pd.DataFrame(dict(a=[], b=[], c=[])))  # type: ignore

        df = task.load_data_frame(required_columns={'a', 'c'})
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(0, df.shape[0])
        self.assertSetEqual({'a', 'b', 'c'}, set(df.columns))

    def test_load_index_only_dataframe(self):
        task = _DummyTask()
        task.load = Mock(return_value=pd.DataFrame(index=range(3)))  # type: ignore

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
        self.assertListEqual(without_task.requires(), [])  # type: ignore

        with_task = _WithTaskInstanceParameter(a_task=without_task)
        self.assertEqual(with_task.requires()['a_task'], without_task)  # type: ignore

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
        task: gokart.TaskOnKart = gokart.TaskOnKart()
        deserialized: gokart.TaskOnKart = luigi.task_register.load_task(None, task.get_task_family(), task.to_str_params())
        self.assertDictEqual(task.to_str_params(), deserialized.to_str_params())

    def test_should_lock_run_when_set(self):
        class _DummyTaskWithLock(gokart.TaskOnKart):
            def run(self):
                self.dump('hello')

        task = _DummyTaskWithLock(redis_host='host', redis_port=123, redis_timeout=180, should_lock_run=True)
        self.assertEqual(task.run.__wrapped__.__name__, 'run')  # type: ignore

    def test_should_fail_lock_run_when_host_unset(self):
        with self.assertRaises(AssertionError):
            gokart.TaskOnKart(redis_port=123, redis_timeout=180, should_lock_run=True)

    def test_should_fail_lock_run_when_port_unset(self):
        with self.assertRaises(AssertionError):
            gokart.TaskOnKart(redis_host='host', redis_timeout=180, should_lock_run=True)


class _DummyTaskWithNonCompleted(gokart.TaskOnKart):
    def dump(self, _obj: Any, _target: Any = None):
        # overrive dump() to do nothing.
        pass

    def run(self):
        self.dump('hello')

    def complete(self):
        return False


class _DummyTaskWithCompleted(gokart.TaskOnKart):
    def dump(self, obj: Any, _target: Any = None):
        # overrive dump() to do nothing.
        pass

    def run(self):
        self.dump('hello')

    def complete(self):
        return True


class TestCompleteCheckAtRun(unittest.TestCase):
    def test_run_when_complete_check_at_run_is_false_and_task_is_not_completed(self):
        task = _DummyTaskWithNonCompleted(complete_check_at_run=False)
        task.dump = Mock()  # type: ignore
        task.run()

        # since run() is called, dump() should be called.
        task.dump.assert_called_once()

    def test_run_when_complete_check_at_run_is_false_and_task_is_completed(self):
        task = _DummyTaskWithCompleted(complete_check_at_run=False)
        task.dump = Mock()  # type: ignore
        task.run()

        # even task is completed, since run() is called, dump() should be called.
        task.dump.assert_called_once()

    def test_run_when_complete_check_at_run_is_true_and_task_is_not_completed(self):
        task = _DummyTaskWithNonCompleted(complete_check_at_run=True)
        task.dump = Mock()  # type: ignore
        task.run()

        # since task is not completed, when run() is called, dump() should be called.
        task.dump.assert_called_once()

    def test_run_when_complete_check_at_run_is_true_and_task_is_completed(self):
        task = _DummyTaskWithCompleted(complete_check_at_run=True)
        task.dump = Mock()  # type: ignore
        task.run()

        # since task is completed, even when run() is called, dump() should not be called.
        task.dump.assert_not_called()


if __name__ == '__main__':
    unittest.main()
