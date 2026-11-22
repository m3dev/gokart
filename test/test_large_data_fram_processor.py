import os
import shutil
import unittest

import numpy as np
import pandas as pd

from gokart.target import LargeDataFrameProcessor
from test.util import _get_temporary_directory


class _NoValuesDataFrame(pd.DataFrame):
    # module level so that to_pickle can pickle it
    @property
    def _constructor(self):
        return _NoValuesDataFrame

    @property
    def values(self):
        raise AssertionError('save must not copy the whole DataFrame into an ndarray')


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

    def test_save_does_not_materialize_values(self):
        file_path = os.path.join(self.temporary_directory, 'test_no_values.zip')
        df = _NoValuesDataFrame(dict(data=np.arange(100, dtype=np.float64)))
        LargeDataFrameProcessor(max_byte=200).save(df, file_path)

        pd.testing.assert_frame_equal(pd.DataFrame(LargeDataFrameProcessor.load(file_path)), pd.DataFrame(df))

    def test_save_splits_uneven_row_count_without_losing_rows(self):
        file_path = os.path.join(self.temporary_directory, 'test_uneven.zip')
        df = pd.DataFrame(dict(data=np.arange(1003, dtype=np.float64)))
        processor = LargeDataFrameProcessor(max_byte=800)
        processor.save(df, file_path)
        chunk_sizes = [len(pd.read_pickle(p)) for _, p in processor._chunk_paths(self.temporary_directory)]

        # 1003 * 8 // 800 + 1 chunks
        self.assertEqual(len(chunk_sizes), 11)
        # same boundaries as np.array_split: the first n % k chunks get one extra row
        self.assertEqual(chunk_sizes, [np.size(c) for c in np.array_split(np.arange(1003), len(chunk_sizes))])
        pd.testing.assert_frame_equal(processor.load(file_path), df)

    def test_save_counts_object_column_payload_when_deciding_split_size(self):
        file_path = os.path.join(self.temporary_directory, 'test_mixed.zip')
        # dtype=object explicitly: pandas 3 would turn the strings into StringDtype.
        # values.nbytes counts 8 bytes per object pointer, so it underestimated the strings themselves.
        df = pd.DataFrame(dict(text=pd.Series(['x' * 1000] * 100, dtype=object), number=np.arange(100, dtype=np.float64)))
        processor = LargeDataFrameProcessor(max_byte=10000)
        processor.save(df, file_path)

        chunk_count = len(processor._chunk_paths(self.temporary_directory))
        old_estimate_chunk_count = df.values.nbytes // 10000 + 1
        self.assertEqual(old_estimate_chunk_count, 1)
        # ~(100 * (1000 + per-str header of ~40-50 B) + 100 * 8) // 10000 + 1; the header size varies by Python version
        self.assertEqual(chunk_count, 11)
        pd.testing.assert_frame_equal(processor.load(file_path), df)

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
