from __future__ import annotations

import os
import tempfile
import unittest
from typing import Callable

import boto3
from luigi import LocalTarget
from moto import mock_aws

from gokart.file_processor import (
    DATAFRAME_FRAMEWORK,
    CsvFileProcessor,
    FeatherFileProcessor,
    GzipFileProcessor,
    JsonFileProcessor,
    NpzFileProcessor,
    PandasCsvFileProcessor,
    ParquetFileProcessor,
    PickleFileProcessor,
    PolarsCsvFileProcessor,
    TextFileProcessor,
    make_file_processor,
)
from gokart.object_storage import ObjectStorage


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


class TestFileProcessorClassSelection(unittest.TestCase):
    def test_processor_selection(self):
        if DATAFRAME_FRAMEWORK == 'polars':
            self.assertTrue(issubclass(CsvFileProcessor, PolarsCsvFileProcessor))
        else:
            self.assertTrue(issubclass(CsvFileProcessor, PandasCsvFileProcessor))


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
