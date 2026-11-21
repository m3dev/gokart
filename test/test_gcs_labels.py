import datetime
import unittest
from typing import Any

from gokart._gcs import _normalize_labels, _trim_to_metadata_size_limit, build_task_state_labels, needs_task_state_labels
from gokart.required_task_output import RequiredTaskOutput


class TestBuildTaskStateLabels(unittest.TestCase):
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
        got = _normalize_labels(None)
        self.assertEqual(got, {})

    def test_normalize_labels_has_value(self):
        got = _normalize_labels(self.task_params)

        self.assertIsInstance(got, dict)
        self.assertIn('param1', got)
        self.assertIn('param2', got)
        self.assertIn('param3', got)
        self.assertIn('param4', got)
        self.assertIn('param5', got)
        self.assertIn('param6', got)

    def test_only_task_params(self):
        got = build_task_state_labels(task_params=self.task_params, custom_labels=None)

        self.assertIsInstance(got, dict)
        self.assertIn('param1', got)
        self.assertIn('param2', got)
        self.assertIn('param3', got)
        self.assertIn('param4', got)
        self.assertIn('param5', got)
        self.assertNotIn('param6', got)

    def test_only_custom_labels(self):
        got = build_task_state_labels(task_params=None, custom_labels=self.custom_labels)

        self.assertIsInstance(got, dict)
        self.assertIn('created_at', got)
        self.assertIn('created_by', got)
        self.assertIn('empty', got)
        self.assertIn('try_num', got)

    def test_with_both_task_params_and_custom_labels(self):
        got = build_task_state_labels(task_params=self.task_params, custom_labels=self.custom_labels)

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

    def test_with_exceeded_size_metadata(self):
        size_exceeded_task_params = {
            'param1': 'a' * 5000,
            'param2': 'b' * 5000,
        }
        want = {
            'param1': 'a' * 5000,
        }
        got = build_task_state_labels(task_params=size_exceeded_task_params)
        self.assertEqual(got, want)

    def test_with_conflicts(self):
        got = build_task_state_labels(task_params=self.task_params_with_conflicts, custom_labels=self.custom_labels)
        self.assertIsInstance(got, dict)
        self.assertIn('created_at', got)
        self.assertIn('created_by', got)
        self.assertIn('empty', got)
        self.assertIn('try_num', got)
        self.assertEqual(got['empty'], 'True')
        self.assertEqual(got['created_by'], 'hoge fuga')
        self.assertEqual(got['param1'], 'a' * 10)

    def test_with_required_task_outputs(self):
        got = build_task_state_labels(
            required_task_outputs=[
                RequiredTaskOutput(task_name='task1', output_path='path/to/output1'),
            ],
        )

        self.assertIsInstance(got, dict)
        self.assertIn('__required_task_outputs', got)
        self.assertEqual(got['__required_task_outputs'], '[{"__gokart_task_name": "task1", "__gokart_output_path": "path/to/output1"}]')

    def test_with_nested_required_task_outputs(self):
        got = build_task_state_labels(
            required_task_outputs={
                'nested_task': {'nest': RequiredTaskOutput(task_name='task1', output_path='path/to/output1')},
            },
        )

        self.assertIsInstance(got, dict)
        self.assertIn('__required_task_outputs', got)
        self.assertEqual(
            got['__required_task_outputs'], '{"nested_task": {"nest": {"__gokart_task_name": "task1", "__gokart_output_path": "path/to/output1"}}}'
        )

    def test_skips_empty_label_names(self):
        # metadata goes into the upload request, so a name GCS may reject must not reach it
        with self.assertLogs('gokart._gcs', level='WARNING'):
            got = build_task_state_labels(task_params={'': '1', 'param': '2'})

        self.assertEqual(got, {'param': '2'})

    def test_trim_to_metadata_size_limit_with_many_labels(self):
        large_labels = {}
        for i in range(100):
            large_labels[f'key_{i}'] = 'x' * 1000

        with self.assertLogs('gokart._gcs', level='WARNING') as logs:
            got = _trim_to_metadata_size_limit(large_labels)

        self.assertEqual(list(got), [f'key_{i}' for i in range(8)])
        self.assertIn("'key_99'", logs.output[0])
        self.assertIn("'key_8'", logs.output[0])
        self.assertNotIn("'key_7'", logs.output[0])

    def test_trim_to_metadata_size_limit_does_not_warn_when_labels_fit(self):
        with self.assertNoLogs('gokart._gcs', level='WARNING'):
            got = _trim_to_metadata_size_limit({'key': 'value'})

        self.assertEqual(got, {'key': 'value'})


class TestNeedsTaskStateLabels:
    def test_gcs_path(self):
        assert needs_task_state_labels('gs://bucket/path/data.pkl')

    def test_non_gcs_and_log_paths(self):
        for path in ['s3://bucket/data.pkl', '/tmp/data.pkl', 'gs://bucket/log/task_log/x.pkl', 'gs://bucket/log/processing_time/x.pkl']:
            assert not needs_task_state_labels(path)
