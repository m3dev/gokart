from __future__ import annotations

import datetime
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]

import gokart
from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient
from gokart.required_task_output import RequiredTaskOutput
from gokart.target import TargetOnKart


class _DummyTaskOnKart(gokart.TaskOnKart[str]):
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

    def test_get_patched_obj_metadata_with_required_task_outputs(self):
        got = GCSObjectMetadataClient._get_patched_obj_metadata(
            {},
            required_task_outputs=[
                RequiredTaskOutput(task_name='task1', output_path='path/to/output1'),
            ],
        )

        self.assertIsInstance(got, dict)
        self.assertIn('__required_task_outputs', got)
        self.assertEqual(got['__required_task_outputs'], '[{"__gokart_task_name": "task1", "__gokart_output_path": "path/to/output1"}]')

    def test_get_patched_obj_metadata_with_nested_required_task_outputs(self):
        got = GCSObjectMetadataClient._get_patched_obj_metadata(
            {},
            required_task_outputs={
                'nested_task': {'nest': RequiredTaskOutput(task_name='task1', output_path='path/to/output1')},
            },
        )

        self.assertIsInstance(got, dict)
        self.assertIn('__required_task_outputs', got)
        self.assertEqual(
            got['__required_task_outputs'], '{"nested_task": {"nest": {"__gokart_task_name": "task1", "__gokart_output_path": "path/to/output1"}}}'
        )

    def test_adjust_gcs_metadata_limit_size_runtime_error(self):
        large_labels = {}
        for i in range(100):
            large_labels[f'key_{i}'] = 'x' * 1000

        GCSObjectMetadataClient._adjust_gcs_metadata_limit_size(large_labels)


class TestGokartTask(unittest.TestCase):
    @patch.object(_DummyTaskOnKart, '_get_output_target')
    def test_mock_target_on_kart(self, mock_get_output_target):
        mock_target = MagicMock(spec=TargetOnKart)
        mock_target.path.return_value = 'gs://bucket/path/data.pkl'
        mock_get_output_target.return_value = mock_target

        task = _DummyTaskOnKart()
        task.dump({'key': 'value'}, mock_target)

        mock_target.dump.assert_called_once_with(
            {'key': 'value'}, lock_at_dump=task._lock_at_dump, task_params={}, custom_labels=None, required_task_outputs=[]
        )


def _blob(bucket: str, key: str) -> Any:
    return storage.Client().bucket(bucket).get_blob(key)


class TestAddTaskStateLabels:
    def _upload(self, bucket: str, key: str, metadata: dict[str, str] | None = None) -> str:
        blob = storage.Client().bucket(bucket).blob(key)
        blob.metadata = metadata
        blob.upload_from_string(b'x')
        return f'gs://{bucket}/{key}'

    def test_adds_labels_and_keeps_existing_metadata(self, gcs_bucket):
        path = self._upload(gcs_bucket, 'data.pkl', metadata={'existing': 'v'})

        GCSObjectMetadataClient.add_task_state_labels(path, task_params={'param': '1'}, custom_labels={'owner': 'me'})

        assert _blob(gcs_bucket, 'data.pkl').metadata == {'existing': 'v', 'param': '1', 'owner': 'me'}

    def test_does_not_patch_when_labels_are_unchanged(self, gcs_bucket):
        path = self._upload(gcs_bucket, 'data.pkl', metadata={'param': '1'})

        with patch('google.cloud.storage.Blob.patch') as blob_patch:
            GCSObjectMetadataClient.add_task_state_labels(path, task_params={'param': '1'})

        blob_patch.assert_not_called()

    def test_deletes_existing_keys_trimmed_to_fit_the_size_limit(self, gcs_bucket):
        # overriding 'a' with a larger value pushes the total over 8 KiB, so the trailing 'b' is trimmed and must be deleted on GCS too
        # GCS merges patched metadata into the existing one and deletes only keys sent as None; the emulator replaces it wholesale,
        # so the patch body is inspected instead of the stored metadata
        path = self._upload(gcs_bucket, 'data.pkl', metadata={'a': 'x' * 4000, 'b': 'y' * 4000})
        sent: list[dict[str, Any]] = []

        def _patch(blob, **kwargs):
            sent.append(dict(blob.metadata))

        with patch('google.cloud.storage.Blob.patch', autospec=True, side_effect=_patch):
            GCSObjectMetadataClient.add_task_state_labels(path, task_params={'a': 'z' * 5000})

        assert sent == [{'a': 'z' * 5000, 'b': None}]

    def test_skips_log_paths(self, gcs_bucket):
        path = self._upload(gcs_bucket, 'log/task_log/data.pkl')

        GCSObjectMetadataClient.add_task_state_labels(path, task_params={'param': '1'})

        assert not _blob(gcs_bucket, 'log/task_log/data.pkl').metadata

    def test_patches_with_metageneration_precondition_and_retries_once_on_conflict(self, gcs_bucket):
        # the emulator ignores preconditions, so the conflict is simulated on the client side
        path = self._upload(gcs_bucket, 'data.pkl')
        original_patch = storage.Blob.patch
        calls: list[dict[str, Any]] = []

        def _patch(blob, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                raise PreconditionFailed('metageneration changed')
            return original_patch(blob, **kwargs)

        with patch('google.cloud.storage.Blob.patch', autospec=True, side_effect=_patch):
            GCSObjectMetadataClient.add_task_state_labels(path, task_params={'param': '1'})

        assert len(calls) == 2
        assert all(c.get('if_metageneration_match') is not None for c in calls)
        assert _blob(gcs_bucket, 'data.pkl').metadata == {'param': '1'}


if __name__ == '__main__':
    unittest.main()
