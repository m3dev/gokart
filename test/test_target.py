import io
import json
import logging
import os
import pickle
import shutil
import unittest
from datetime import datetime
from typing import Any, cast
from unittest.mock import patch

import boto3
import luigi
import luigi.mock
import numpy as np
import pandas as pd
import pytest
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]
from matplotlib import pyplot
from moto import mock_aws

from gokart.conflict_prevention_lock.task_lock import make_task_lock_params
from gokart.file_processor import make_file_processor
from gokart.in_memory import make_in_memory_target
from gokart.required_task_output import RequiredTaskOutput
from gokart.target import (
    SingleFileTarget,
    _LargeDataFrameProcessor,
    is_persistent_target,
    make_model_target,
    make_target,
    target_last_modification_time,
    target_path,
)
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
        loaded = target.load()

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
        pd.DataFrame({'x': range(10), 'y': range(10)}).plot.scatter(x='x', y='y')
        pyplot.savefig(figure_binary)
        figure_binary.seek(0)
        file_path = os.path.join(self.temporary_directory, 'test.png')
        target = make_target(file_path=file_path, unique_id=None)
        target.dump(figure_binary.read())

        loaded = target.load()
        self.assertGreater(len(loaded), 1000)  # any binary

    def test_save_and_load_csv(self):
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        file_path = os.path.join(self.temporary_directory, 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_tsv(self):
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        file_path = os.path.join(self.temporary_directory, 'test.tsv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_parquet(self):
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        file_path = os.path.join(self.temporary_directory, 'test.parquet')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_feather(self):
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}, index=pd.Index([33, 44], name='object_index'))
        file_path = os.path.join(self.temporary_directory, 'test.feather')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_feather_without_store_index_in_feather(self):
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]}, index=pd.Index([33, 44], name='object_index')).reset_index()
        file_path = os.path.join(self.temporary_directory, 'test.feather')

        target = make_target(file_path=file_path, unique_id=None, store_index_in_feather=False)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_last_modified_time(self):
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
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

        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
        file_path = os.path.join('s3://test/', 'test.feather')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    @mock_aws
    def test_save_on_s3_parquet(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
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

    @patch('gokart.target.build_task_state_labels')
    def test_model_target_on_local_does_not_add_task_state_labels(self, mock_build_task_state_labels):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        target = make_model_target(
            file_path=file_path, temporary_directory=self.temporary_directory, save_function=self._save_function, load_function=self._load_function
        )

        target.dump(1, task_params={'a': '1'})

        mock_build_task_state_labels.assert_not_called()

    def _make_large_data_frame_target(self, file_path: str, load_function=None):
        processor = _LargeDataFrameProcessor(max_byte=800)
        return make_model_target(
            file_path=file_path,
            temporary_directory=self.temporary_directory,
            save_function=processor.save,
            load_function=load_function or processor.load,
        )

    def _dump_large_data_frame_while(self, file_path: str, df: pd.DataFrame, callback) -> None:
        # Runs callback while the chunks of this dump are on disk, which reproduces both a concurrent dump and a dump that crashed before cleanup.
        processor = _LargeDataFrameProcessor(max_byte=800)

        def save_function(obj, path):
            processor.save(obj, path)
            callback()

        make_model_target(file_path=file_path, temporary_directory=self.temporary_directory, save_function=save_function, load_function=processor.load).dump(df)

    def test_large_data_frame_dump_ignores_chunks_of_another_dump(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame({'data': np.arange(10, dtype=np.float64)})
        target = self._make_large_data_frame_target(file_path)
        loaded = []

        def dump_and_load():
            target.dump(df)
            loaded.append(target.load())

        self._dump_large_data_frame_while(file_path, pd.DataFrame({'data': np.arange(1200, dtype=np.float64)}), dump_and_load)

        pd.testing.assert_frame_equal(loaded[0], df)

    def test_large_data_frame_load_ignores_chunks_of_another_dump(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame({'data': np.arange(10, dtype=np.float64)})
        target = self._make_large_data_frame_target(file_path)
        target.dump(df)
        loaded = []

        self._dump_large_data_frame_while(file_path, pd.DataFrame({'data': np.arange(1200, dtype=np.float64)}), lambda: loaded.append(target.load()))

        pd.testing.assert_frame_equal(loaded[0], df)

    def test_large_data_frame_load_keeps_chunks_of_another_dump(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        target = self._make_large_data_frame_target(file_path)
        target.dump(pd.DataFrame({'data': np.arange(10, dtype=np.float64)}))
        df = pd.DataFrame({'data': np.arange(1200, dtype=np.float64)})

        self._dump_large_data_frame_while(file_path, df, target.load)

        pd.testing.assert_frame_equal(target.load(), df)

    def test_large_data_frame_load_keeps_files_of_another_load(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame({'data': np.arange(1200, dtype=np.float64)})
        another_target = self._make_large_data_frame_target(file_path)
        another_target.dump(df)

        def load_function(path):
            another_target.load()
            return _LargeDataFrameProcessor.load(path)

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
        obj = pd.DataFrame({'a': [1, 2], 'b': [3, 4]})
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

    def test_model_target_dump_attaches_labels_to_zip(self, gcs_bucket, tmp_path):
        target = make_model_target(
            file_path=f'gs://{gcs_bucket}/model.zip',
            temporary_directory=str(tmp_path),
            save_function=lambda obj, path: pickle.dump(obj, open(path, 'wb')),
            load_function=lambda path: pickle.load(open(path, 'rb')),
        )

        target.dump(1, task_params={'a': '1'}, custom_labels={'b': '2'})

        assert target.load() == 1
        blob = storage.Client().bucket(gcs_bucket).get_blob('model.zip')
        assert blob is not None
        assert blob.metadata == {'a': '1', 'b': '2'}

    def test_model_target_dump_attaches_labels_in_the_upload_request(self, gcs_bucket, tmp_path):
        target = make_model_target(
            file_path=f'gs://{gcs_bucket}/model.zip',
            temporary_directory=str(tmp_path),
            save_function=lambda obj, path: pickle.dump(obj, open(path, 'wb')),
            load_function=lambda path: pickle.load(open(path, 'rb')),
        )

        with patch('google.cloud.storage.Blob.reload') as reload, patch('google.cloud.storage.Blob.patch') as blob_patch:
            target.dump(1, custom_labels={'b': '2'})

        reload.assert_not_called()
        blob_patch.assert_not_called()
        assert self._metadata(gcs_bucket, 'model.zip') == {'b': '2'}

    @staticmethod
    def _metadata(bucket: str, key: str) -> dict[str, str] | None:
        blob = storage.Client().bucket(bucket).get_blob(key)
        assert blob is not None
        return blob.metadata

    def test_dump_attaches_required_task_outputs_label(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        target.dump(1, required_task_outputs={'a': RequiredTaskOutput(task_name='TaskA', output_path='gs://b/a.pkl')})

        metadata = self._metadata(gcs_bucket, 'data.pkl')
        assert metadata is not None
        assert json.loads(metadata['__required_task_outputs']) == {'a': {'__gokart_task_name': 'TaskA', '__gokart_output_path': 'gs://b/a.pkl'}}

    def test_dump_prefers_custom_labels_and_skips_empty_values(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        target.dump(1, task_params={'owner': 'task', 'empty': '', 'param': '1'}, custom_labels={'owner': 'me'})

        assert self._metadata(gcs_bucket, 'data.pkl') == {'owner': 'me', 'param': '1'}

    def test_dump_does_not_label_log_paths(self, gcs_bucket):
        target = make_target(file_path=self._log_path(gcs_bucket, 'test.pkl'), unique_id=None)

        target.dump(1, task_params={'param': '1'})

        assert not self._metadata(gcs_bucket, 'log/processing_time/test.pkl')

    def test_dump_trims_labels_to_the_metadata_size_limit(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        target.dump(1, task_params={f'param{i}': 'x' * 1000 for i in range(10)})

        metadata = self._metadata(gcs_bucket, 'data.pkl')
        assert metadata is not None
        assert 0 < sum(len(k.encode()) + len(v.encode()) for k, v in metadata.items()) <= 8 * 1024
        assert target.load() == 1

    def test_dump_attaches_labels_in_the_upload_request(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        with patch('google.cloud.storage.Blob.reload') as reload, patch('google.cloud.storage.Blob.patch') as blob_patch:
            target.dump(1, custom_labels={'owner': 'me'})

        reload.assert_not_called()
        blob_patch.assert_not_called()
        assert self._metadata(gcs_bucket, 'data.pkl') == {'owner': 'me'}

    def test_dump_with_invalid_required_task_outputs_raises_before_upload(self, gcs_bucket):
        target = make_target(file_path=f'gs://{gcs_bucket}/data.pkl', unique_id=None)

        with pytest.raises(TypeError):
            target.dump(1, required_task_outputs=cast(Any, object()))

        assert not target.exists()

    def test_dump_to_gcs_path_on_non_gcs_target_skips_labels(self, caplog):
        # SingleFileTarget accepts any FileSystemTarget, so a gs:// path does not imply that open() takes metadata
        path = 'gs://bucket/data.pkl'
        processor = make_file_processor(path)
        target = SingleFileTarget(
            target=luigi.mock.MockTarget(path, format=processor.format()),
            processor=processor,
            task_lock_params=make_task_lock_params(file_path=path, unique_id=None),
        )

        with caplog.at_level(logging.DEBUG, logger='gokart.target'):
            target.dump(1, custom_labels={'owner': 'me'})

        assert target.load() == 1
        assert 'skip task state labels' in caplog.text

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


class TestIsPersistent:
    def test_file_target(self, tmp_path) -> None:
        assert make_target(str(tmp_path / 'file.pkl')).is_persistent()

    def test_model_target(self, tmp_path) -> None:
        target = make_model_target(str(tmp_path / 'model.zip'), temporary_directory=str(tmp_path / 'tmp'), save_function=None, load_function=None)
        assert target.is_persistent()

    def test_in_memory_target(self) -> None:
        assert not make_in_memory_target('gs://bucket/memory.pkl').is_persistent()


class TestIsPersistentTarget:
    def test_target_on_kart(self, tmp_path) -> None:
        assert is_persistent_target(make_target(str(tmp_path / 'file.pkl')))
        assert not is_persistent_target(make_in_memory_target('memory.pkl'))

    def test_plain_luigi_target_is_persistent(self) -> None:
        # output() may return targets that are not TargetOnKart despite its annotation; they have no is_persistent().
        assert is_persistent_target(luigi.LocalTarget('plain'))


class TestTargetPath:
    def test_target_on_kart(self, tmp_path) -> None:
        target = make_target(str(tmp_path / 'file.pkl'))
        assert target_path(target) == target.path()

    def test_plain_luigi_target(self, tmp_path) -> None:
        # luigi.LocalTarget has path as an attribute, while TargetOnKart has path() as a method.
        assert target_path(luigi.LocalTarget(str(tmp_path / 'plain.txt'))) == str(tmp_path / 'plain.txt')

    def test_custom_luigi_target_with_path_method(self) -> None:
        class _CustomTarget(luigi.Target):
            def exists(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
                return True

            def path(self) -> str:
                return 'custom'

        assert target_path(_CustomTarget()) == 'custom'


class TestTargetLastModificationTime:
    def test_target_on_kart(self, tmp_path) -> None:
        target = make_target(str(tmp_path / 'file.pkl'))
        target.dump('x')
        assert target_last_modification_time(target) == target.last_modification_time()

    def test_plain_luigi_target(self, tmp_path) -> None:
        path = tmp_path / 'plain.txt'
        path.write_text('plain')
        os.utime(path, (1_000, 1_000))
        assert target_last_modification_time(luigi.LocalTarget(str(path))) == datetime.fromtimestamp(1_000)


if __name__ == '__main__':
    unittest.main()
