"""Tests for file processor factory function."""

from __future__ import annotations

import unittest

from gokart.file_processor import (
    CsvFileProcessor,
    FeatherFileProcessor,
    GzipFileProcessor,
    JsonFileProcessor,
    NpzFileProcessor,
    ParquetFileProcessor,
    TextFileProcessor,
    make_file_processor,
)


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
