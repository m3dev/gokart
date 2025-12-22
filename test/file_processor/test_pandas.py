"""Tests for pandas-specific file processors."""

from __future__ import annotations

import tempfile
import unittest

import pandas as pd
import pytest
from luigi import LocalTarget

from gokart.file_processor import CsvFileProcessor, FeatherFileProcessor, JsonFileProcessor


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


class TestJsonFileProcessor:
    @pytest.mark.parametrize(
        'orient,input_data,expected_json',
        [
            pytest.param(
                None,
                pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
                '{"A":{"0":1,"1":2,"2":3},"B":{"0":4,"1":5,"2":6}}',
                id='With Default Orient for DataFrame',
            ),
            pytest.param(
                'records',
                pd.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
                '{"A":1,"B":4}\n{"A":2,"B":5}\n{"A":3,"B":6}\n',
                id='With Records Orient for DataFrame',
            ),
            pytest.param(None, {'A': [1, 2, 3], 'B': [4, 5, 6]}, '{"A":{"0":1,"1":2,"2":3},"B":{"0":4,"1":5,"2":6}}', id='With Default Orient for Dict'),
            pytest.param('records', {'A': [1, 2, 3], 'B': [4, 5, 6]}, '{"A":1,"B":4}\n{"A":2,"B":5}\n{"A":3,"B":6}\n', id='With Records Orient for Dict'),
            pytest.param(None, {}, '{}', id='With Default Orient for Empty Dict'),
            pytest.param('records', {}, '\n', id='With Records Orient for Empty Dict'),
        ],
    )
    def test_dump_and_load_json(self, orient, input_data, expected_json):
        processor = JsonFileProcessor(orient=orient)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(input_data, f)
            with local_target.open('r') as f:
                loaded_df = processor.load(f)
                f.seek(0)
                loaded_json = f.read().decode('utf-8')

        assert loaded_json == expected_json

        df_input = pd.DataFrame(input_data)
        pd.testing.assert_frame_equal(df_input, loaded_df)


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
