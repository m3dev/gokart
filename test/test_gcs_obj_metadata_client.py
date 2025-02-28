# mypy: ignore-errors
# The reason why this file ignore mypy errors is luigi.Parameter specification.
# There are no ways to get luigi.Parameter instance.
# In _get_patched_obj_metadata(), params arguments type is list[tuple[str, Any, luigi.Parameter]],
# so, even if in this file, we need to cast params to list[tuple[str, Any, luigi.Parameter]].
# But by luigi's specification, we get string value, which is parameter value instead of luigi.Parameter instance.

import datetime
import enum
import unittest
from unittest.mock import MagicMock, patch

import luigi

import gokart
from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient
from gokart.target import TargetOnKart


class _DummyTask(luigi.Task):
    task_namespace = __name__

    def run(self):
        print(f'{self.__class__.__name__} has been executed')

    def complete(self):
        return True


class _DummyTaskOnKart(gokart.TaskOnKart):
    task_namespace = __name__

    def run(self):
        self.dump('Dummy TaskOnKart')


class _DummyEnum(enum.Enum):
    hoge = 1
    fuga = 2
    geho = 3


class TestGCSObjectMetadataClient(unittest.TestCase):
    def test_get_patched_normal_parameter(self):
        param1 = luigi.Parameter(default='param1_value')
        params = [
            ('param1', 'param1_value', param1),
        ]
        want = {
            'param1': 'param1_value',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_parameter(self):
        param1 = luigi.OptionalParameter(default=None)
        param2 = luigi.OptionalParameter(default='optional')

        params = [
            ('param1', None, param1),
            ('param2', 'optional', param2),
        ]
        want = {
            'param2': 'optional',
        }

        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_str_parameter(self):
        param1 = luigi.OptionalStrParameter(default=None)
        param2 = luigi.OptionalStrParameter(default='optional string')
        params = [
            ('param1', None, param1),
            ('param2', 'optional string', param2),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertNotIn('param1', got)
        self.assertIn('param2', got)
        self.assertEqual('optional string', got['param2'])

    def test_get_patched_date_parameter(self):
        param1 = luigi.DateParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3).strftime(param1.date_format), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('6' in got['param1'])
        self.assertFalse('4' in got['param1'])

    def test_get_patched_month_parameter(self):
        param1 = luigi.MonthParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3).strftime(param1.date_format), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('5' in got['param1'])
        self.assertFalse('6' in got['param1'])

    def test_get_patched_year_parameter(self):
        param1 = luigi.YearParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3).strftime(param1.date_format), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('1789' in got['param1'])
        self.assertFalse('5' in got['param1'])

    def test_get_patched_date_hour_parameter(self):
        param1 = luigi.DateHourParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3).strftime(param1.date_format), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('4' in got['param1'])
        self.assertFalse('2' in got['param1'])

    def test_get_patched_date_minute_parameter(self):
        param1 = luigi.DateMinuteParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3).strftime(param1.date_format), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('2' in got['param1'])
        self.assertFalse('3' in got['param1'])

    def test_get_patched_date_second_parameter(self):
        param1 = luigi.DateSecondParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3).strftime(param1.date_format), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('3' in got['param1'])

    def test_get_patched_int_parameter(self):
        param1 = luigi.IntParameter(default=100)
        params = [
            ('param1', 100, param1),
        ]
        want = {
            'param1': '100',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_int_parameter(self):
        param1 = luigi.OptionalIntParameter(default=None)
        param2 = luigi.OptionalIntParameter(default=10)
        params = [('param1', None, param1), ('param2', 10, param2)]
        want = {
            'param2': '10',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_float_parameter(self):
        param1 = luigi.FloatParameter(default=1.234)
        params = [
            ('param1', 1.234, param1),
        ]
        want = {
            'param1': '1.234',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_float_parameter(self):
        param1 = luigi.OptionalFloatParameter(default=None)
        param2 = luigi.OptionalFloatParameter(default=12.345)
        params = [
            ('param1', None, param1),
            ('param2', 12.345, param2),
        ]
        want = {
            'param2': '12.345',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_bool_parameter(self):
        param1 = luigi.BoolParameter(default=False)
        params = [
            ('param1', False, param1),
        ]
        want = {
            'param1': 'False',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_bool_parameter(self):
        param1 = luigi.OptionalBoolParameter(default=None)
        param2 = luigi.OptionalBoolParameter(default=True)
        params = [
            ('param1', None, param1),
            ('param2', True, param2),
        ]
        want = {
            'param2': 'True',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_date_interval_parameter(self):
        param1 = luigi.DateIntervalParameter(default=datetime.datetime(1789, 5, 6, 4, 2, 3))
        params = [
            ('param1', datetime.datetime(1789, 5, 6, 4, 2, 3), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertEqual(got['param1'], '1789-05-06 04:02:03')

    def test_get_patched_time_delta_parameter(self):
        param1 = luigi.TimeDeltaParameter(default=datetime.timedelta(hours=1))
        params = [
            ('param1', datetime.timedelta(hours=1), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertEqual(got['param1'], '1:00:00')

    def test_get_patched_task_parameter(self):
        param1 = luigi.TaskParameter(default=_DummyTask())
        params = [
            ('param1', _DummyTask(), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('_DummyTask' in got['param1'])

    def test_get_patched_enum_parameter(self):
        param1 = luigi.EnumParameter(enum=_DummyEnum, default=_DummyEnum.hoge)
        params = [
            ('param1', _DummyEnum.hoge, param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('_DummyEnum.hoge' in got['param1'])

    def test_get_patched_enum_list_parameter(self):
        param1 = luigi.EnumParameter(enum=_DummyEnum, default=[_DummyEnum.hoge, _DummyEnum.fuga, _DummyEnum.geho])
        params = [
            ('param1', [_DummyEnum.hoge, _DummyEnum.fuga, _DummyEnum.geho], param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('_DummyEnum.hoge' in got['param1'])
        self.assertTrue('_DummyEnum.fuga' in got['param1'])
        self.assertTrue('_DummyEnum.geho' in got['param1'])

    def test_get_patched_dict_parameter(self):
        param1 = luigi.DictParameter(default={'color': 'red', 'id': 123, 'is_test': True})
        params = [
            ('param1', {'color': 'red', 'id': 123, 'is_test': True}, param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('color' in got['param1'])
        self.assertTrue('red' in got['param1'])
        self.assertTrue('id' in got['param1'])
        self.assertTrue('123' in got['param1'])
        self.assertTrue('is_test' in got['param1'])
        self.assertTrue('True' in got['param1'])

    def test_get_patched_optional_dict_parameter(self):
        param1 = luigi.OptionalDictParameter(default=None)
        param2 = luigi.OptionalDictParameter(default={'color': 'red', 'id': 123, 'is_test': True})
        params = [
            ('param1', None, param1),
            ('param2', {'color': 'red', 'id': 123, 'is_test': True}, param2),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertNotIn('param1', got)
        self.assertIn('param2', got)
        self.assertIsInstance(got['param2'], str)
        self.assertTrue('color' in got['param2'])
        self.assertTrue('red' in got['param2'])
        self.assertTrue('id' in got['param2'])
        self.assertTrue('123' in got['param2'])
        self.assertTrue('is_test' in got['param2'])
        self.assertTrue('True' in got['param2'])

    def test_get_patched_list_parameter(self):
        param1 = luigi.ListParameter(default=[1, 2, 3, 4, 5])
        params = [
            ('param1', [1, 2, 3, 4, 5], param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('1' in got['param1'])
        self.assertTrue('2' in got['param1'])
        self.assertTrue('3' in got['param1'])
        self.assertTrue('4' in got['param1'])
        self.assertTrue('5' in got['param1'])

    def test_get_patched_optional_list_parameter(self):
        param1 = luigi.OptionalListParameter(default=None)
        param2 = luigi.ListParameter(default=[1, 2, 3, 4, 5])
        params = [
            ('param1', None, param1),
            ('param2', [1, 2, 3, 4, 5], param2),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertNotIn('param1', got)
        self.assertIn('param2', got)
        self.assertIsInstance(got['param2'], str)
        self.assertTrue('1' in got['param2'])
        self.assertTrue('2' in got['param2'])
        self.assertTrue('3' in got['param2'])
        self.assertTrue('4' in got['param2'])
        self.assertTrue('5' in got['param2'])

    def test_get_patched_tuple_parameter(self):
        param1 = luigi.TupleParameter(default=('hoge', 1, True, {'a': 'b'}))
        params = [
            ('param1', ('hoge', 1, True, {'a': 'b'}), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('hoge' in got['param1'])
        self.assertTrue('1' in got['param1'])
        self.assertTrue('True' in got['param1'])
        self.assertTrue('a' in got['param1'])
        self.assertTrue('b' in got['param1'])

    def test_get_patched_optional_tuple_parameter(self):
        param1 = luigi.OptionalTupleParameter(default=None)
        param2 = luigi.TupleParameter(default=('hoge', 1, True, {'a': 'b'}))
        params = [
            ('param1', None, param1),
            ('param2', ('hoge', 1, True, {'a': 'b'}), param2),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertNotIn('param1', got)
        self.assertIn('param2', got)
        self.assertIsInstance(got['param2'], str)
        self.assertTrue('hoge' in got['param2'])
        self.assertTrue('1' in got['param2'])
        self.assertTrue('True' in got['param2'])
        self.assertTrue('a' in got['param2'])
        self.assertTrue('b' in got['param2'])

    def test_get_patched_path_parameter(self):
        param1 = luigi.PathParameter(default='/hoge/fuga')
        params = [
            ('param1', '/hoge/fuga', param1),
        ]
        want = {
            'param1': '/hoge/fuga',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_path_parameter(self):
        param1 = luigi.OptionalPathParameter(default=None)
        param2 = luigi.OptionalPathParameter(default='/hoge/fuga')
        params = [
            ('param1', None, param1),
            ('param2', '/hoge/fuga', param2),
        ]
        want = {
            'param2': '/hoge/fuga',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    # gokart specific parameter test
    def test_get_patched_task_instance_parameter(self):
        param1 = gokart.TaskInstanceParameter(default=_DummyTaskOnKart())
        params = [
            ('param1', str(_DummyTaskOnKart()), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertIn('param1', got)
        self.assertIsInstance(got['param1'], str)
        self.assertTrue('_DummyTaskOnKart' in got['param1'])

    def test_get_patched_list_task_instance_parameter(self):
        param1 = gokart.ListTaskInstanceParameter(default=[_DummyTaskOnKart(), _DummyTaskOnKart()])
        params = [
            ('param1', str([_DummyTaskOnKart(), _DummyTaskOnKart()]), param1),
        ]
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertTrue(got['param1'].count('_DummyTaskOnKart') == 2)

    def test_get_patched_explicit_bool_parameter(self):
        param1 = gokart.ExplicitBoolParameter(default=True)
        params = [
            ('param1', str(True), param1),
        ]
        want = {
            'param1': str(True),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_obj_metadata_with_exceeded_size_metadata(self):
        param1 = luigi.Parameter(default='a' * 5000)
        param2 = luigi.Parameter(default='b' * 5000)

        params = [
            ('param1', 'a' * 5000, param1),
            ('param2', 'b' * 5000, param2),
        ]

        want = {
            'param1': 'a' * 5000,
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(got, want)


class TestGokartTask(unittest.TestCase):
    @patch('gokart.target.TargetOnKart.dump')
    @patch.object(_DummyTaskOnKart, '_get_output_target')
    def test_mock_target_on_kart(self, mock_get_output_target, mock_dump):
        mock_target = MagicMock(spec=TargetOnKart)
        mock_get_output_target.return_value = mock_target

        task = _DummyTaskOnKart()
        task.dump({'key': 'value'}, mock_target)

        mock_target.dump.assert_called_once_with({'key': 'value'}, lock_at_dump=task._lock_at_dump, params=[])


if __name__ == '__main__':
    unittest.main()
