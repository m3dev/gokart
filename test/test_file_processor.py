import os
import tempfile
import unittest
from typing import Callable

import boto3
import pandas as pd
from luigi import LocalTarget
from moto import mock_aws
from parameterized import parameterized

from gokart.file_processor import (
    CsvFileProcessor,
    FeatherFileProcessor,
    GzipFileProcessor,
    JsonFileProcessor,
    NpzFileProcessor,
    ParquetFileProcessor,
    PickleFileProcessor,
    TextFileProcessor,
    make_file_processor,
)
from gokart.object_storage import ObjectStorage


class TestCsvFileProcessor(unittest.TestCase):
    def test_dump_csv_with_utf8(self):
        df = pd.DataFrame({'あ': [1, 2, 3], 'い': [4, 5, 6]})
        processor = CsvFileProcessor()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            # read with utf-8 to check if the file is dumped with utf8
            loaded_df = pd.read_csv(temp_path, encoding='utf-8')
            pd.testing.assert_frame_equal(df, loaded_df)

    def test_dump_csv_with_cp932(self):
        df = pd.DataFrame({'あ': [1, 2, 3], 'い': [4, 5, 6]})
        processor = CsvFileProcessor(encoding='cp932')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            # read with cp932 to check if the file is dumped with cp932
            loaded_df = pd.read_csv(temp_path, encoding='cp932')
            pd.testing.assert_frame_equal(df, loaded_df)

    def test_load_csv_with_utf8(self):
        df = pd.DataFrame({'あ': [1, 2, 3], 'い': [4, 5, 6]})
        processor = CsvFileProcessor()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'
            df.to_csv(temp_path, encoding='utf-8', index=False)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                # read with utf-8 to check if the file is dumped with utf8
                loaded_df = processor.load(f)
                pd.testing.assert_frame_equal(df, loaded_df)

    def test_load_csv_with_cp932(self):
        df = pd.DataFrame({'あ': [1, 2, 3], 'い': [4, 5, 6]})
        processor = CsvFileProcessor(encoding='cp932')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'
            df.to_csv(temp_path, encoding='cp932', index=False)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                # read with cp932 to check if the file is dumped with cp932
                loaded_df = processor.load(f)
                pd.testing.assert_frame_equal(df, loaded_df)


target_orients = [None, 'records']


class TestJsonFileProcessor(unittest.TestCase):
    @parameterized.expand([(orient,) for orient in target_orients])
    def test_dump_and_load_dataframe(self, orient):
        df = pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]})
        processor = JsonFileProcessor(orient=orient)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

        pd.testing.assert_frame_equal(df, loaded_df)

    @parameterized.expand([(orient,) for orient in target_orients])
    def test_dump_and_load_dict(self, orient):
        data = {'A': [1, 2, 3], 'B': [4, 5, 6]}
        processor = JsonFileProcessor(orient=orient)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(data, f)
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

        pd.testing.assert_frame_equal(pd.DataFrame(data), loaded_df)

    @parameterized.expand([(orient,) for orient in target_orients])
    def test_load_empty_dict(self, orient):
        data: dict = {}
        processor = JsonFileProcessor(orient=orient)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            with open(temp_path, 'w') as f:
                processor.dump(data, f)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

        pd.testing.assert_frame_equal(pd.DataFrame(data), loaded_df)

    @parameterized.expand([(orient,) for orient in target_orients])
    def test_load_empty_dataframe(self, orient):
        data = pd.DataFrame()
        processor = JsonFileProcessor(orient=orient)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            with open(temp_path, 'w') as f:
                processor.dump(data, f)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

        pd.testing.assert_frame_equal(data, loaded_df)


class TestPickleFileProcessor(unittest.TestCase):
    def test_dump_and_load_normal_obj(self):
        var = 'abc'
        processor = PickleFileProcessor()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.pkl'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(var, f)
            with local_target.open('r') as f:
                loaded = processor.load(f)

        self.assertEqual(loaded, var)

    def test_dump_and_load_class(self):
        import functools

        def plus1(func: Callable[..., int]) -> Callable[..., int]:
            @functools.wraps(func)
            def wrapped() -> int:
                ret = func()
                return ret + 1

            return wrapped

        class A:
            def __init__(self) -> None:
                self.run = plus1(self.run)  # type: ignore

            def run(self) -> int:  # type: ignore
                return 1

        obj = A()
        processor = PickleFileProcessor()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.pkl'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(obj, f)
            with local_target.open('r') as f:
                loaded = processor.load(f)

        self.assertEqual(loaded.run(), obj.run())

    @mock_aws
    def test_dump_and_load_with_readables3file(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')
        file_path = os.path.join('s3://test/', 'test.pkl')

        var = 'abc'
        processor = PickleFileProcessor()

        target = ObjectStorage.get_object_storage_target(file_path, processor.format())
        with target.open('w') as f:
            processor.dump(var, f)
        with target.open('r') as f:
            loaded = processor.load(f)

        self.assertEqual(loaded, var)


class TestFeatherFileProcessor(unittest.TestCase):
    def test_feather_should_return_same_dataframe(self):
        df = pd.DataFrame({'a': [1]})
        processor = FeatherFileProcessor(store_index_in_feather=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            pd.testing.assert_frame_equal(df, loaded_df)

    def test_feather_should_save_index_name(self):
        df = pd.DataFrame({'a': [1]}, index=pd.Index([1], name='index_name'))
        processor = FeatherFileProcessor(store_index_in_feather=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            pd.testing.assert_frame_equal(df, loaded_df)

    def test_feather_should_raise_error_index_name_is_None(self):
        df = pd.DataFrame({'a': [1]}, index=pd.Index([1], name='None'))
        processor = FeatherFileProcessor(store_index_in_feather=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                with self.assertRaises(AssertionError):
                    processor.dump(df, f)


class TestMakeFileProcessor(unittest.TestCase):
    def test_make_file_processor_with_txt_extension(self):
        processor = make_file_processor('test.txt', store_index_in_feather=False)
        self.assertIsInstance(processor, TextFileProcessor)

    def test_make_file_processor_with_csv_extension(self):
        processor = make_file_processor('test.csv', store_index_in_feather=False)
        self.assertIsInstance(processor, CsvFileProcessor)

    def test_make_file_processor_with_gz_extension(self):
        processor = make_file_processor('test.gz', store_index_in_feather=False)
        self.assertIsInstance(processor, GzipFileProcessor)

    def test_make_file_processor_with_json_extension(self):
        processor = make_file_processor('test.json', store_index_in_feather=False)
        self.assertIsInstance(processor, JsonFileProcessor)

    def test_make_file_processor_with_ndjson_extension(self):
        processor = make_file_processor('test.ndjson', store_index_in_feather=False)
        self.assertIsInstance(processor, JsonFileProcessor)

    def test_make_file_processor_with_npz_extension(self):
        processor = make_file_processor('test.npz', store_index_in_feather=False)
        self.assertIsInstance(processor, NpzFileProcessor)

    def test_make_file_processor_with_parquet_extension(self):
        processor = make_file_processor('test.parquet', store_index_in_feather=False)
        self.assertIsInstance(processor, ParquetFileProcessor)

    def test_make_file_processor_with_feather_extension(self):
        processor = make_file_processor('test.feather', store_index_in_feather=True)
        self.assertIsInstance(processor, FeatherFileProcessor)

    def test_make_file_processor_with_unsupported_extension(self):
        with self.assertRaises(AssertionError):
            make_file_processor('test.unsupported', store_index_in_feather=False)
