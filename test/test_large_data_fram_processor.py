import os
import shutil
import unittest

import numpy as np
import pandas as pd

from gokart.target import LargeDataFrameProcessor


def _get_temporary_directory():
    return os.path.abspath(os.path.join(os.path.dirname(__name__), 'temporary'))


class LargeDataFrameProcessorTest(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree(_get_temporary_directory(), ignore_errors=True)

    def test_save_and_load(self):
        file_path = os.path.join(_get_temporary_directory(), 'test.zip')
        df = pd.DataFrame(dict(data=np.random.uniform(0, 1, size=int(1e+6))))
        processor = LargeDataFrameProcessor(max_byte=int(1e+6))
        processor.save(df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, df, check_like=True)

    def test_save_and_load_empty(self):
        file_path = os.path.join(_get_temporary_directory(), 'test_with_empty.zip')
        df = pd.DataFrame()
        processor = LargeDataFrameProcessor(max_byte=int(1e+6))
        processor.save(df, file_path)
        loaded = processor.load(file_path)

        pd.testing.assert_frame_equal(loaded, df, check_like=True)


if __name__ == '__main__':
    unittest.main()
