import datetime
import unittest

import luigi

from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient


class TestGCSObjectMetadataClient(unittest.TestCase):
    def test_get_patched_obj_metadata(self):
        param0 = luigi.OptionalParameter(default=None)
        param1 = luigi.Parameter(default='param1_value')
        param2 = luigi.IntParameter(default=100)
        param3 = luigi.BoolParameter(default=False)
        param4 = luigi.OptionalStrParameter(default='optional string')
        param5 = luigi.DateParameter(default=datetime.date.today())
        param6 = luigi.MonthParameter(default=datetime.date.today())
        param7 = luigi.YearParameter(default=datetime.date.today())
        param8 = luigi.DateHourParameter(default=datetime.date.today())
        param9 = luigi.DateMinuteParameter(default=datetime.date.today())
        param10 = luigi.DateSecondParameter(default=datetime.date.today())
        user_provided_gcs_labels = luigi.DictParameter(default={'hoge': 'fuga', '1': 2})

        params = [
            ('param0', None, param0),
            ('param1', 'param1_value', param1),
            ('param2', 100, param2),
            ('param3', False, param3),
            ('param4', 'optional string', param4),
            ('param5', datetime.date.today().strftime(param5.date_format), param5),
            ('param6', datetime.date.today().strftime(param6.date_format), param6),
            ('param7', datetime.date.today().strftime(param7.date_format), param7),
            ('param8', datetime.date.today().strftime(param8.date_format), param8),
            ('param9', datetime.date.today().strftime(param9.date_format), param9),
            ('param10', datetime.date.today().strftime(param10.date_format), param10),
            ('user_provided_gcs_labels', {'hoge': 'fuga', '1': 2}, user_provided_gcs_labels),
        ]
        want = {
            'hoge': 'fuga',
            '1': '2',
            'param1': 'param1_value',
            'param2': '100',
            'param3': 'False',
            'param4': 'optional string',
            'param5': datetime.date.today().strftime(param5.date_format),
            'param6': datetime.date.today().strftime(param6.date_format),
            'param7': datetime.date.today().strftime(param7.date_format),
            'param8': datetime.date.today().strftime(param8.date_format),
            'param9': datetime.date.today().strftime(param9.date_format),
            'param10': datetime.date.today().strftime(param10.date_format),
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, params=params)
        self.assertEqual(got, want)


if __name__ == '__main__':
    unittest.main()
