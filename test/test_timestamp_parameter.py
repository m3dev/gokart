import datetime
import unittest

import pytest
from luigi.cmdline_parser import CmdlineParser

from gokart import TaskOnKart, TimestampParameter


class TimestampParameterTaskWithoutDefault(TaskOnKart):
    task_namespace = __name__
    timestamp: datetime.datetime = TimestampParameter()

    def run(self):
        self.dump(self.timestamp)


class TimestampParameterTaskWithDefault(TaskOnKart):
    task_namespace = __name__
    timestamp: datetime.datetime = TimestampParameter(default=datetime.datetime(2025, 2, 21, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=9))))

    def run(self):
        self.dump(self.timestamp)


class TimestampParameterTest(unittest.TestCase):
    def setUp(self):
        self.default_datetime = datetime.datetime(2025, 2, 21, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
        self.default_datetime_str = '2025-02-21T120000Z+0900'

    def test_default(self):
        with CmdlineParser.global_instance([f'{__name__}.TimestampParameterTaskWithDefault']) as cp:
            assert cp.get_task_obj().timestamp == self.default_datetime

    def test_parse_param(self):
        with CmdlineParser.global_instance([f'{__name__}.TimestampParameterTaskWithDefault', '--timestamp', '2024-01-20T110000Z+0000']) as cp:
            assert cp.get_task_obj().timestamp == datetime.datetime(2024, 1, 20, 11, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=0)))

    def test_value_error_without_timezone_input(self):
        with pytest.raises(ValueError):
            with CmdlineParser.global_instance([f'{__name__}.TimestampParameterTaskWithoutDefault', '--timestamp', '2025-02-21T120000']) as cp:
                cp.get_task_obj()

    def test_parse_method(self):
        actual = TimestampParameter().parse(self.default_datetime_str)
        expected = self.default_datetime
        self.assertEqual(actual, expected)

    def test_serialize_method(self):
        actual = TimestampParameter().serialize(self.default_datetime)
        expected = self.default_datetime_str
        self.assertEqual(actual, expected)

    def test_serialize_task(self):
        task = TimestampParameterTaskWithoutDefault(timestamp=self.default_datetime)
        actual = str(task)
        expected = f'(timestamp={self.default_datetime_str})'
        self.assertTrue(actual.endswith(expected))


if __name__ == '__main__':
    unittest.main()
