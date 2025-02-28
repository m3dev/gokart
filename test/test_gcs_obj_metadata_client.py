import datetime
import enum
import unittest

import luigi

from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient


class _DummyTask(luigi.Task):
    def run(self):
        print(f"{self.__class__.__name__} has been executed")

    def complete(self):
        return True

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
        param2 = luigi.OptionalParameter(default="optional")

        params = [
            ('param1', None, param1),
            ('param2', 'optional', param2),
        ]
        want = {
            'param2' : 'optional',
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
        want = {
            'param2': 'optional string',
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_date_parameter(self):
        param1 = luigi.DateParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format), param1),
        ]
        want = {
            'param1': datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_month_parameter(self):
        param1 = luigi.MonthParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format), param1),
        ]
        want = {
            'param1': datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_year_parameter(self):
        param1 = luigi.YearParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format), param1),
        ]
        want = {
            'param1': datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_date_hour_parameter(self):
        param1 = luigi.DateHourParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format), param1),
        ]
        want = {
            'param1': datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_date_minute_parameter(self):
        param1 = luigi.DateMinuteParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format), param1),
        ]
        want = {
            'param1': datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_date_second_parameter(self):
        param1 = luigi.DateSecondParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format), param1),
        ]
        want = {
            'param1': datetime.datetime(2025, 2, 27, 0, 0).strftime(param1.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

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
        params = [
            ('param1', None, param1),
            ('param2', 10, param2)
        ]
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
        param1 = luigi.DateIntervalParameter(default=datetime.datetime(2025, 2, 27, 0, 0))
        params = [
            ('param1', datetime.datetime(2025, 2, 27, 0, 0), param1),
        ]
        want = {
            'param1': str(datetime.datetime(2025, 2, 27, 0, 0)),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_time_delta_parameter(self):
        param1 = luigi.TimeDeltaParameter(default=datetime.timedelta(hours=1))
        params = [
            ('param1', datetime.timedelta(hours=1), param1),
        ]
        want = {
            'param1': str(datetime.timedelta(hours=1)),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_task_parameter(self):
        param1 = luigi.TaskParameter(default=_DummyTask())
        params = [
            ('param1', _DummyTask(), param1),
        ]
        want = {
            'param1': str(_DummyTask()),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_enum_parameter(self):
        param1 = luigi.EnumParameter(enum=_DummyEnum, default=_DummyEnum.hoge)
        params = [
            ('param1', _DummyEnum.hoge, param1),
        ]
        want = {
            'param1': str(_DummyEnum.hoge),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_enum_list_parameter(self):
        param1 = luigi.EnumParameter(enum=_DummyEnum, default=[_DummyEnum.hoge, _DummyEnum.fuga, _DummyEnum.geho])
        params = [
            ('param1', [_DummyEnum.hoge, _DummyEnum.fuga, _DummyEnum.geho], param1),
        ]
        want = {
            'param1': str([_DummyEnum.hoge, _DummyEnum.fuga, _DummyEnum.geho]),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_dict_parameter(self):
        param1 = luigi.DictParameter(default={"color":"red", "id": 123, "is_test": True})
        params = [
            ('param1', {"color":"red", "id": 123, "is_test": True}, param1),
        ]
        want = {
            'param1': str({"color":"red", "id": 123, "is_test": True}),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_dict_parameter(self):
        param1 = luigi.OptionalDictParameter(default=None)
        param2 = luigi.OptionalDictParameter(default={"color": "red", "id": 123, "is_test": True})
        params = [
            ('param1', None, param1),
            ('param2', {"color": "red", "id": 123, "is_test": True}, param2),
        ]
        want = {
            'param2': str({"color": "red", "id": 123, "is_test": True}),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_list_parameter(self):
        param1 = luigi.ListParameter(default=[1, 2, 3, 4, 5])
        params = [
            ('param1', [1, 2, 3, 4, 5], param1),
        ]
        want = {
            'param1': str([1, 2, 3, 4, 5]),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_list_parameter(self):
        param1 = luigi.OptionalListParameter(default=None)
        param2 = luigi.ListParameter(default=[1, 2, 3, 4, 5])
        params = [
            ('param1', None, param1),
            ('param2', [1, 2, 3, 4, 5], param2),
        ]
        want = {
            'param2': str([1, 2, 3, 4, 5]),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_tuple_parameter(self):
        param1 = luigi.TupleParameter(default=('hoge', 1, True, {'a': 'b'}))
        params = [
            ('param1', ('hoge', 1, True, {'a': 'b'}), param1),
        ]
        want = {
            'param1': str(('hoge', 1, True, {'a': 'b'})),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_tuple_parameter(self):
        param1 = luigi.OptionalTupleParameter(default=None)
        param2 = luigi.TupleParameter(default=('hoge', 1, True, {'a': 'b'}))
        params = [
            ('param1', None, param1),
            ('param2', ('hoge', 1, True, {'a': 'b'}), param2),
        ]
        want = {
            'param2': str(('hoge', 1, True, {'a': 'b'})),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_path_parameter(self):
        param1 = luigi.PathParameter(default="/hoge/fuga")
        params = [
            ('param1', "/hoge/fuga", param1),
        ]
        want = {
            'param1': "/hoge/fuga",
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_optional_path_parameter(self):
        param1 = luigi.OptionalPathParameter(default=None)
        param2 = luigi.OptionalPathParameter(default="/hoge/fuga")
        params = [
            ('param1', None, param1),
            ('param2', "/hoge/fuga", param2),
        ]
        want = {
            'param2': "/hoge/fuga",
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(want, got)

    def test_get_patched_obj_metadata_with_exceeded_size_metadata(self):
        param1 = luigi.Parameter(default='a'*5000)
        param2 = luigi.Parameter(default='b'*5000)

        params = [
            ('param1', 'a'*5000, param1),
            ('param2', 'b' * 5000, param2),
        ]

        want = {
            'param1': 'a'*5000,
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(got, want)

if __name__ == '__main__':
    unittest.main()
