import tempfile
import unittest
from luigi import LocalTarget

import pandas as pd

from gokart.file_processor import CsvFileProcessor


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
