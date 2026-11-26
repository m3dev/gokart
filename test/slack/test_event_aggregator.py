import logging
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

import luigi

import gokart
from gokart.slack import EventAggregator
from test.util import PlainLuigiTask


class _KartTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def run(self):
        self.dump('done')


class _FailingKartTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def run(self):
        raise RuntimeError('boom')


class TestEventAggregator(unittest.TestCase):
    def setUp(self) -> None:
        self.aggregator = EventAggregator()
        self.aggregator.set_handlers()
        self.addCleanup(self.aggregator.remove_handlers)
        tmp_dir = TemporaryDirectory()
        self.addCleanup(tmp_dir.cleanup)
        self.tmp_path = Path(tmp_dir.name)

    def test_records_task_on_kart_events(self):
        task = _KartTask(workspace_directory=str(self.tmp_path))
        self.assertTrue(luigi.build([task], local_scheduler=True, log_level='CRITICAL'))

        self.assertEqual(self.aggregator.get_summary(), 'Success: 1; Failure: 0')
        self.assertIn(f'_KartTask:[{task.make_unique_id()}]', self.aggregator.get_event_list())

    def test_records_task_on_kart_failure(self):
        task = _FailingKartTask(workspace_directory=str(self.tmp_path))
        self.assertFalse(luigi.build([task], local_scheduler=True, log_level='CRITICAL'))

        self.assertEqual(self.aggregator.get_summary(), 'Success: 0; Failure: 1')
        self.assertIn(f'Task: _FailingKartTask:[{task.make_unique_id()}]; Exception: boom', self.aggregator.get_event_list())

    def test_ignores_plain_luigi_task(self):
        # https://github.com/m3dev/gokart/issues/718
        # WHY: no log_level here. If this is the first luigi.build in the process, log_level='CRITICAL' would raise the level of
        # luigi-interface inside assertNoLogs, and the 'Error in event callback' log would be dropped instead of failing the test.
        with self.assertNoLogs('luigi-interface', level=logging.ERROR):
            self.assertTrue(luigi.build([PlainLuigiTask(path=str(self.tmp_path / 'plain.txt'))], local_scheduler=True))

        self.assertEqual(self.aggregator.get_summary(), 'Success: 0; Failure: 0')

    def test_remove_handlers_stops_recording(self):
        self.aggregator.remove_handlers()
        self.addCleanup(self.aggregator.set_handlers)  # so that the cleanup in setUp can remove them again

        self.assertTrue(luigi.build([_KartTask(workspace_directory=str(self.tmp_path))], local_scheduler=True, log_level='CRITICAL'))

        self.assertEqual(self.aggregator.get_summary(), 'Success: 0; Failure: 0')

    def test_remove_handlers_twice_does_not_raise(self):
        self.aggregator.remove_handlers()
        self.aggregator.remove_handlers()
        self.addCleanup(self.aggregator.set_handlers)  # so that the cleanup in setUp can remove them again
