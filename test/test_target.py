import io
import os
import shutil
import unittest
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
from gokart.file_processor import _ChunkedLargeFileReader
from gokart.redis_lock import RedisParams
from matplotlib import pyplot
from moto import mock_s3

from gokart.target import make_target, make_model_target


def _get_temporary_directory():
    return os.path.abspath(os.path.join(os.path.dirname(__name__), 'temporary'))


class LocalTargetTest(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree(_get_temporary_directory(), ignore_errors=True)

    def test_save_and_load_pickle_file(self):
        obj = 1
        file_path = os.path.join(_get_temporary_directory(), 'test.pkl')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        with unittest.mock.patch('gokart.file_processor._ChunkedLargeFileReader', wraps=_ChunkedLargeFileReader) as monkey:
            loaded = target.load()
            monkey.assert_called()

        self.assertEqual(loaded, obj)

    def test_save_and_load_text_file(self):
        obj = 1
        file_path = os.path.join(_get_temporary_directory(), 'test.txt')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, [str(obj)], msg='should save an object as List[str].')

    def test_save_and_load_gzip(self):
        obj = 1
        file_path = os.path.join(_get_temporary_directory(), 'test.gz')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, [str(obj)], msg='should save an object as List[str].')

    def test_save_and_load_npz(self):
        obj = np.ones(shape=10, dtype=np.float32)
        file_path = os.path.join(_get_temporary_directory(), 'test.npz')
        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        np.testing.assert_almost_equal(obj, loaded)

    def test_save_and_load_figure(self):
        figure_binary = io.BytesIO()
        pd.DataFrame(dict(x=range(10), y=range(10))).plot.scatter(x='x', y='y')
        pyplot.savefig(figure_binary)
        figure_binary.seek(0)
        file_path = os.path.join(_get_temporary_directory(), 'test.png')
        target = make_target(file_path=file_path, unique_id=None)
        target.dump(figure_binary.read())

        loaded = target.load()
        self.assertGreater(len(loaded), 1000)  # any binary

    def test_save_and_load_csv(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(_get_temporary_directory(), 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_tsv(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(_get_temporary_directory(), 'test.tsv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_parquet(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(_get_temporary_directory(), 'test.parquet')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_save_and_load_feather(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(_get_temporary_directory(), 'test.feather')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_frame_equal(loaded, obj)

    def test_last_modified_time(self):
        obj = pd.DataFrame(dict(a=[1, 2], b=[3, 4]))
        file_path = os.path.join(_get_temporary_directory(), 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        t = target.last_modification_time()
        self.assertIsInstance(t, datetime)

    def test_last_modified_time_without_file(self):
        file_path = os.path.join(_get_temporary_directory(), 'test.csv')
        target = make_target(file_path=file_path, unique_id=None)
        with self.assertRaises(FileNotFoundError):
            target.last_modification_time()

    def test_save_pandas_series(self):
        obj = pd.Series(data=[1, 2], name='column_name')
        file_path = os.path.join(_get_temporary_directory(), 'test.csv')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        pd.testing.assert_series_equal(loaded['column_name'], obj)


class S3TargetTest(unittest.TestCase):
    @mock_s3
    def test_save_on_s3(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = 1
        file_path = os.path.join('s3://test/', 'test.pkl')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, obj)

    @mock_s3
    def test_last_modified_time(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = 1
        file_path = os.path.join('s3://test/', 'test.pkl')

        target = make_target(file_path=file_path, unique_id=None)
        target.dump(obj)
        t = target.last_modification_time()
        self.assertIsInstance(t, datetime)

    @mock_s3
    def test_last_modified_time_without_file(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        file_path = os.path.join('s3://test/', 'test.pkl')
        target = make_target(file_path=file_path, unique_id=None)
        with self.assertRaises(FileNotFoundError):
            target.last_modification_time()


class ModelTargetTest(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree(_get_temporary_directory(), ignore_errors=True)

    @staticmethod
    def _save_function(obj, path):
        make_target(file_path=path).dump(obj)

    @staticmethod
    def _load_function(path):
        return make_target(file_path=path).load()

    def test_model_target_on_local(self):
        obj = 1
        file_path = os.path.join(_get_temporary_directory(), 'test.zip')

        target = make_model_target(file_path=file_path,
                                   temporary_directory=_get_temporary_directory(),
                                   save_function=self._save_function,
                                   load_function=self._load_function)

        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, obj)

    @mock_s3
    def test_model_target_on_s3(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        obj = 1
        file_path = os.path.join('s3://test/', 'test.zip')

        target = make_model_target(file_path=file_path,
                                   temporary_directory=_get_temporary_directory(),
                                   save_function=self._save_function,
                                   load_function=self._load_function)

        target.dump(obj)
        loaded = target.load()

        self.assertEqual(loaded, obj)


if __name__ == '__main__':
    unittest.main()
