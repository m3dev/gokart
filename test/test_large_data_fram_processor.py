import os
import shutil
import unittest

import numpy as np
import pandas as pd

from gokart.target import LargeDataFrameProcessor
from test.util import _get_temporary_directory


class LargeDataFrameProcessorTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = _get_temporary_directory()

    def tearDown(self):
        shutil.rmtree(self.temporary_directory, ignore_errors=True)

    def test_save_and_load(self):
        file_path = os.path.join(self.temporary_directory, 'test.zip')
        df = pd.DataFrame(dict(data=np.random.uniform(0, 1, size=int(1e6))))
        processor = LargeDataFrameProcessor(max_byte=int(1e6))
        processor.save(df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, df)

    def test_save_and_load_keeps_row_order_with_more_than_ten_chunks(self):
        file_path = os.path.join(self.temporary_directory, 'test_with_many_chunks.zip')
        df = pd.DataFrame(dict(data=np.arange(1200, dtype=np.float64)))
        processor = LargeDataFrameProcessor(max_byte=800)
        processor.save(df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, df)

    def test_save_and_load_when_split_size_exceeds_row_count(self):
        file_path = os.path.join(self.temporary_directory, 'test_with_tiny_max_byte.zip')
        df = pd.DataFrame(dict(data=np.arange(2, dtype=np.float64)))
        processor = LargeDataFrameProcessor(max_byte=1)
        processor.save(df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, df)

    def test_invalid_max_byte_raises_value_error(self):
        for max_byte in (0, -1):
            with self.subTest(max_byte=max_byte):
                with self.assertRaises(ValueError):
                    LargeDataFrameProcessor(max_byte=max_byte)

    def test_save_removes_stale_chunks_from_previous_larger_save(self):
        file_path = os.path.join(self.temporary_directory, 'test_with_stale.zip')
        large_df = pd.DataFrame(dict(data=np.arange(1200, dtype=np.float64)))
        small_df = pd.DataFrame(dict(data=np.arange(10, dtype=np.float64)))
        LargeDataFrameProcessor(max_byte=800).save(large_df, file_path)
        processor = LargeDataFrameProcessor(max_byte=int(1e6))
        processor.save(small_df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, small_df)

    def test_save_keeps_unrelated_files(self):
        file_path = os.path.join(self.temporary_directory, 'test_with_unrelated.zip')
        unrelated = os.path.join(self.temporary_directory, 'data_notes.pkl')
        os.makedirs(self.temporary_directory, exist_ok=True)
        with open(unrelated, 'w') as f:
            f.write('keep')
        LargeDataFrameProcessor(max_byte=int(1e6)).save(pd.DataFrame(dict(data=[1.0])), file_path)

        self.assertTrue(os.path.exists(unrelated))

    def test_save_and_load_empty(self):
        file_path = os.path.join(self.temporary_directory, 'test_with_empty.zip')
        df = pd.DataFrame()
        processor = LargeDataFrameProcessor(max_byte=int(1e6))
        processor.save(df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, df, check_like=True)


if __name__ == '__main__':
    unittest.main()
