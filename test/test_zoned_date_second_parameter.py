import datetime
import unittest

from luigi.cmdline_parser import CmdlineParser

from gokart import TaskOnKart, ZonedDateSecondParameter


class ZonedDateSecondParameterTaskWithoutDefault(TaskOnKart):
    task_namespace = __name__
    dt: datetime.datetime = ZonedDateSecondParameter()

    def run(self):
        self.dump(self.dt)


class ZonedDateSecondParameterTaskWithDefault(TaskOnKart):
    task_namespace = __name__
    dt: datetime.datetime = ZonedDateSecondParameter(default=datetime.datetime(2025, 2, 21, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=9))))

    def run(self):
        self.dump(self.dt)


class ZonedDateSecondParameterTest(unittest.TestCase):
    def setUp(self):
        self.default_datetime = datetime.datetime(2025, 2, 21, 12, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
        self.default_datetime_str = '2025-02-21T12:00:00+09:00'

    def test_default(self):
        with CmdlineParser.global_instance([f'{__name__}.ZonedDateSecondParameterTaskWithDefault']) as cp:
            assert cp.get_task_obj().dt == self.default_datetime

    def test_parse_param_with_tz_suffix(self):
        with CmdlineParser.global_instance([f'{__name__}.ZonedDateSecondParameterTaskWithDefault', '--dt', '2024-01-20T11:00:00+09:00']) as cp:
            assert cp.get_task_obj().dt == datetime.datetime(2024, 1, 20, 11, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))

    def test_parse_param_with_Z_suffix(self):
        with CmdlineParser.global_instance([f'{__name__}.ZonedDateSecondParameterTaskWithDefault', '--dt', '2024-01-20T11:00:00Z']) as cp:
            assert cp.get_task_obj().dt == datetime.datetime(2024, 1, 20, 11, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=0)))

    def test_parse_param_without_timezone_input(self):
        with CmdlineParser.global_instance([f'{__name__}.ZonedDateSecondParameterTaskWithoutDefault', '--dt', '2025-02-21T12:00:00']) as cp:
            assert cp.get_task_obj().dt == datetime.datetime(2025, 2, 21, 12, 0, 0, tzinfo=None)

    def test_parse_method(self):
        actual = ZonedDateSecondParameter().parse(self.default_datetime_str)
        expected = self.default_datetime
        self.assertEqual(actual, expected)

    def test_serialize_task(self):
        task = ZonedDateSecondParameterTaskWithoutDefault(dt=self.default_datetime)
        actual = str(task)
        expected = f'(dt={self.default_datetime_str})'
        self.assertTrue(actual.endswith(expected))


if __name__ == '__main__':
    unittest.main()
