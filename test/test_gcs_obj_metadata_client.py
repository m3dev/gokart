from __future__ import annotations

import datetime
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import gokart
from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient
from gokart.target import TargetOnKart


class _DummyTaskOnKart(gokart.TaskOnKart):
    task_namespace = __name__

    def run(self):
        self.dump('Dummy TaskOnKart')


class TestGCSObjectMetadataClient(unittest.TestCase):
    def setUp(self):
        self.task_params: dict[str, str] = {
            'param1': 'a' * 1000,
            'param2': str(1000),
            'param3': str({'key1': 'value1', 'key2': True, 'key3': 2}),
            'param4': str([1, 2, 3, 4, 5]),
            'param5': str(datetime.datetime(year=2025, month=1, day=2, hour=3, minute=4, second=5)),
            'param6': '',
        }
        self.custom_labels: dict[str, Any] = {
            'created_at': datetime.datetime(year=2025, month=1, day=2, hour=3, minute=4, second=5),
            'created_by': 'hoge fuga',
            'empty': True,
            'try_num': 3,
        }

        self.task_params_with_conflicts = {
            'empty': 'False',
            'created_by': 'fuga hoge',
            'param1': 'a' * 10,
        }

    def test_normalize_labels_not_empty(self):
        got = GCSObjectMetadataClient._normalize_labels(None)
        self.assertEqual(got, {})

    def test_normalize_labels_has_value(self):
        got = GCSObjectMetadataClient._normalize_labels(self.task_params)

        self.assertIsInstance(got, dict)
        self.assertIsInstance(got, dict)
        self.assertIn('param1', got)
        self.assertIn('param2', got)
        self.assertIn('param3', got)
        self.assertIn('param4', got)
        self.assertIn('param5', got)
        self.assertIn('param6', got)

    def test_get_patched_obj_metadata_only_task_params(self):
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, task_params=self.task_params, custom_labels=None)

        self.assertIsInstance(got, dict)
        self.assertIn('param1', got)
        self.assertIn('param2', got)
        self.assertIn('param3', got)
        self.assertIn('param4', got)
        self.assertIn('param5', got)
        self.assertNotIn('param6', got)

    def test_get_patched_obj_metadata_only_custom_labels(self):
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, task_params=None, custom_labels=self.custom_labels)

        self.assertIsInstance(got, dict)
        self.assertIn('created_at', got)
        self.assertIn('created_by', got)
        self.assertIn('empty', got)
        self.assertIn('try_num', got)

    def test_get_patched_obj_metadata_with_both_task_params_and_custom_labels(self):
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, task_params=self.task_params, custom_labels=self.custom_labels)

        self.assertIsInstance(got, dict)
        self.assertIn('param1', got)
        self.assertIn('param2', got)
        self.assertIn('param3', got)
        self.assertIn('param4', got)
        self.assertIn('param5', got)
        self.assertNotIn('param6', got)
        self.assertIn('created_at', got)
        self.assertIn('created_by', got)
        self.assertIn('empty', got)
        self.assertIn('try_num', got)

    def test_get_patched_obj_metadata_with_exceeded_size_metadata(self):
        size_exceeded_task_params = {
            'param1': 'a' * 5000,
            'param2': 'b' * 5000,
        }
        want = {
            'param1': 'a' * 5000,
        }
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, task_params=size_exceeded_task_params)
        self.assertEqual(got, want)

    def test_get_patched_obj_metadata_with_conflicts(self):
        got = GCSObjectMetadataClient._get_patched_obj_metadata({}, task_params=self.task_params_with_conflicts, custom_labels=self.custom_labels)
        self.assertIsInstance(got, dict)
        self.assertIn('created_at', got)
        self.assertIn('created_by', got)
        self.assertIn('empty', got)
        self.assertIn('try_num', got)
        self.assertEqual(got['empty'], 'True')
        self.assertEqual(got['created_by'], 'hoge fuga')
        self.assertEqual(got['param1'], 'a' * 10)


class TestGokartTask(unittest.TestCase):
    @patch.object(_DummyTaskOnKart, '_get_output_target')
    def test_mock_target_on_kart(self, mock_get_output_target):
        mock_target = MagicMock(spec=TargetOnKart)
        mock_get_output_target.return_value = mock_target

        task = _DummyTaskOnKart()
        task.dump({'key': 'value'}, mock_target)
        mock_target.dump.assert_called_once_with({'key': 'value'}, lock_at_dump=task._lock_at_dump, task_params={}, custom_labels=None)


if __name__ == '__main__':
    unittest.main()
