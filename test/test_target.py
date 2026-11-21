import io
import logging
import os
import pickle
import shutil
import unittest
from datetime import datetime
from unittest.mock import patch

import boto3
import numpy as np
import pandas as pd
import pytest
from google.api_core.exceptions import PreconditionFailed
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]
from matplotlib import pyplot
from moto import mock_aws

from gokart.file_processor.base import _ChunkedLargeFileReader
from gokart.target import LargeDataFrameProcessor, make_model_target, make_target
from test.util import _get_temporary_directory


class LocalTargetTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = _get_temporary_directory()

    def tearDown(self):
        shutil.rmtree(self.temporary_directory, ignore_errors=True)

    def test_save_and_load_pickle_file(self):
        obj = 1
        file_path = os.path.join(self.temporary_directory, 'test.pkl')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        with patch('gokart.file_processor.base._ChunkedLargeFileReader', wraps=_ChunkedLargeFileReader) as monkey:
            loaded = target.load()
            monkey.assert_called()

        self.assertEqual(loaded, obj)

    def test_save_and_load_text_file(self):
        obj = 1
        file_path = os.path.join(self.temporary_directory, 'test.txt')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, [str(obj)], msg='should save an object as List[str].')

    def test_save_and_load_gzip(self):
        obj = 1
        file_path = os.path.join(self.temporary_directory, 'test.gz')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, [str(obj)], msg='should save an object as List[str].')

    def test_save_and_load_npz(self):
        obj = np.ones(shape=10, dtype=np.float32)
        file_path = os.path.join(self.temporary_directory, 'test.npz')
        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        np.testing.assert_almost_equal(obj, loaded)

    def test_save_and_load_figure(self):
        figure_binary = io.BytesIO()
        pd.DataFrame(dict(x=range(10), y=range(10))).plot.scatter(x='x', y='y')
        pyplot.savefig(figure_binary)
        figure_binary.seek(0)
        file_path = os.path.join(self.temporary_directory, 'test.png')
        target = make_target(file_path=file_path, unique_id=None)
        target.dump(figure_binary.read())

        loaded = target.load()
        self.assertGreater(len(loaded), 1000)  # any binary

    def test_save_and_load_csv(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(self.temporary_directory, 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_tsv(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(self.temporary_directory, 'test.tsv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_parquet(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(self.temporary_directory, 'test.parquet')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_feather(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]), index=pd.Index([33, 44], name='object_index'))
        file_path = os.path.join(self.temporary_directory, 'test.feather')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_feather_without_store_index_in_feather(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]), index=pd.Index([33, 44], name='object_index')).reset_index()
        file_path = os.path.join(self.temporary_directory, 'test.feather')

        target = make_target(file_path=file_path, unique_id=None, store_index_in_feather=False)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_last_modified_time(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(self.temporary_directory, 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        t = target.last_modification_time()
        self.assertIsInstance(t, datetime)

    def test_last_modified_time_without_file(self):
        file_path = os.path.join(self.temporary_directory, 'test.csv')
        target = make_target(file_path=file_path, unique_id=None)
        with self.assertRaises(FileNotFoundError):
            target.last_modification_time()

    def test_save_pandas_series(self):
        obj = pd.Series(data=[1, 2], name='column_name')
        file_path = os.path.join(self.temporary_directory, 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_series_equal(loaded['column_name'], obj)

    def test_dump_with_lock(self):
        with patch('gokart.target.wrap_dump_with_lock') as wrap_with_lock_mock:
            obj = 1
            file_path = os.path.join(self.temporary_directory, 'test.pkl')
            target = make_target(file_path=file_path, unique_id=None)
            target.dump(obj, lock_at_dump=True)

            wrap_with_lock_mock.assert_called_once()

    def test_dump_without_lock(self):
        with patch('gokart.target.wrap_dump_with_lock') as wrap_with_lock_mock:
            obj = 1
            file_path = os.path.join(self.temporary_directory, 'test.pkl')
            target = make_target(file_path=file_path, unique_id=None)
            target.dump(obj, lock_at_dump=False)

            wrap_with_lock_mock.assert_not_called()


class S3TargetTest(unittest.TestCase):
    @mock_aws
    def test_save_on_s3(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = 1
        file_path = os.path.join('s3://test/', 'test.pkl')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, obj)

    @mock_aws
    def test_last_modified_time(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = 1
        file_path = os.path.join('s3://test/', 'test.pkl')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        t = target.last_modification_time()
        self.assertIsInstance(t, datetime)

    @mock_aws
    def test_last_modified_time_without_file(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        file_path = os.path.join('s3://test/', 'test.pkl')
        target = make_target(file_path=file_path, unique_id=None)
        with self.assertRaises(FileNotFoundError):
            target.last_modification_time()

    @mock_aws
    def test_save_on_s3_feather(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join('s3://test/', 'test.feather')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    @mock_aws
    def test_save_on_s3_parquet(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join('s3://test/', 'test.parquet')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)


class ModelTargetTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = _get_temporary_directory()

    def tearDown(self):
        shutil.rmtree(self.temporary_directory, ignore_errors=True)

    @staticmethod
    def _save_function(obj, path):
        make_target(file_path=path).dump(obj)

    @staticmethod
    def _load_function(path):
        return make_target(file_path=path).load()

    def test_model_target_on_local(self):
        obj = 1
        file_path = os.path.join(self.temporary_directory, 'test.zip')

        target = make_model_target(
            file_path=file_path, temporary_directory=self.temporary_directory, save_function=self._save_function, load_function=self._load_function
        )

        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, obj)

    @patch('gokart.gcs_config.GCSConfig.get_gcs_client')
    def test_model_target_on_gcs_adds_task_state_labels_to_zip(self, mock_get_gcs_client):
        client = mock_get_gcs_client.return_value
        client.client.objects.return_value.get.return_value.execute.return_value = {'metadata': {}}
        target = make_model_target(
            file_path='gs://bucket/test.zip', temporary_directory=self.temporary_directory, save_function=self._save_function, load_function=self._load_function
        )

        target.dump(1, task_params={'a': '1'}, custom_labels={'b': '2'})

        client.put.assert_called_once()
        patch_kwargs = client.client.objects.return_value.patch.call_args.kwargs
        self.assertEqual(patch_kwargs['object'], 'test.zip')
        self.assertEqual(patch_kwargs['body']['metadata'], {'a': '1', 'b': '2'})

    @patch('gokart.target.GCSObjectMetadataClient.add_task_state_labels')
    def test_model_target_on_local_does_not_add_task_state_labels(self, mock_add_task_state_labels):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        target = make_model_target(
            file_path=file_path, temporary_directory=self.temporary_directory, save_function=self._save_function, load_function=self._load_function
        )

        target.dump(1, task_params={'a': '1'})

        mock_add_task_state_labels.assert_not_called()

    def _make_large_data_frame_target(self, file_path: str, load_function=None):
        processor = LargeDataFrameProcessor(max_byte=800)
        return make_model_target(
            file_path=file_path,
            temporary_directory=self.temporary_directory,
            save_function=processor.save,
            load_function=load_function or processor.load,
        )

    def _dump_large_data_frame_while(self, file_path: str, df: pd.DataFrame, callback) -> None:
        # Runs callback while the chunks of this dump are on disk, which reproduces both a concurrent dump and a dump that crashed before cleanup.
        processor = LargeDataFrameProcessor(max_byte=800)

        def save_function(obj, path):
            processor.save(obj, path)
            callback()

        make_model_target(file_path=file_path, temporary_directory=self.temporary_directory, save_function=save_function, load_function=processor.load).dump(df)

    def test_large_data_frame_dump_ignores_chunks_of_another_dump(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame(dict(data=np.arange(10, dtype=np.float64)))
        target = self._make_large_data_frame_target(file_path)
        loaded = []

        def dump_and_load():
            target.dump(df)
            loaded.append(target.load())

        self._dump_large_data_frame_while(file_path, pd.DataFrame(dict(data=np.arange(1200, dtype=np.float64))), dump_and_load)

        pd.testing.assert_frame_equal(loaded[0], df)

    def test_large_data_frame_load_ignores_chunks_of_another_dump(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame(dict(data=np.arange(10, dtype=np.float64)))
        target = self._make_large_data_frame_target(file_path)
        target.dump(df)
        loaded = []

        self._dump_large_data_frame_while(file_path, pd.DataFrame(dict(data=np.arange(1200, dtype=np.float64))), lambda: loaded.append(target.load()))

        pd.testing.assert_frame_equal(loaded[0], df)

    def test_large_data_frame_load_keeps_chunks_of_another_dump(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        target = self._make_large_data_frame_target(file_path)
        target.dump(pd.DataFrame(dict(data=np.arange(10, dtype=np.float64))))
        df = pd.DataFrame(dict(data=np.arange(1200, dtype=np.float64)))

        self._dump_large_data_frame_while(file_path, df, target.load)

        pd.testing.assert_frame_equal(target.load(), df)

    def test_large_data_frame_load_keeps_files_of_another_load(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame(dict(data=np.arange(1200, dtype=np.float64)))
        another_target = self._make_large_data_frame_target(file_path)
        another_target.dump(df)

        def load_function(path):
            another_target.load()
            return LargeDataFrameProcessor.load(path)

        target = self._make_large_data_frame_target(file_path, load_function=load_function)

        pd.testing.assert_frame_equal(target.load(), df)

    @mock_aws
    def test_model_target_on_s3(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = 1
        file_path = os.path.join('s3://test/', 'test.zip')

        target = make_model_target(
            file_path=file_path, temporary_directory=self.temporary_directory, save_function=self._save_function, load_function=self._load_function
        )

        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, obj)


class TestGCSTarget:
    # log paths skip GCS label patching, which keeps these tests to the object I/O itself
    @staticmethod
    def _log_path(bucket: str, file_name: str) -> str:
        return f'gs://{bucket}/log/processing_time/{file_name}'

    def test_load_pickle(self, gcs_bucket):
        storage.Client().bucket(gcs_bucket).blob('data.pkl').upload_from_string(pickle.dumps({'a': 1}))

        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        assert target.exists()
        assert target.load() == {'a': 1}

    def test_dump_and_load(self, gcs_bucket):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        for file_name in ['test.pkl', 'test.csv', 'test.feather', 'test.parquet']:
            target = make_target(file_path=self._log_path(gcs_bucket, file_name), unique_id=None)

            target.dump(obj)

            pd.testing.assert_frame_equal(target.load(), obj)

    def test_dump_attaches_labels(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        target.dump(1, task_params={'param': '1'}, custom_labels={'owner': 'me'})

        assert target.load() == 1
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.pkl')
        assert blob is not None
        assert blob.metadata == {'owner': 'me', 'param': '1'}

    def test_dump_succeeds_when_labeling_fails(self, gcs_bucket, caplog):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        # labels are best-effort: the data is already stored when they are attached
        with (
            patch('google.cloud.storage.Blob.patch', side_effect=PreconditionFailed('metageneration changed')),
            caplog.at_level(logging.WARNING, logger='gokart.gcs_obj_metadata_client'),
        ):
            target.dump(1, custom_labels={'owner': 'me'})

        assert target.load() == 1
        assert 'failed to add labels' in caplog.text

    def test_remove(self, gcs_bucket):
        target = make_target(file_path=self._log_path(gcs_bucket, 'test.pkl'), unique_id=None)
        target.dump(1)

        target.remove()

        assert not target.exists()

    def test_last_modification_time_is_timezone_aware_datetime(self, gcs_bucket):
        target = make_target(file_path=self._log_path(gcs_bucket, 'test.pkl'), unique_id=None)
        target.dump(1)

        t = target.last_modification_time()

        assert isinstance(t, datetime)
        assert t.tzinfo is not None

    def test_last_modification_time_without_file(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/missing.pkl', unique_id=None)

        with pytest.raises(FileNotFoundError):
            target.last_modification_time()


if __name__ == '__main__':
    unittest.main()
