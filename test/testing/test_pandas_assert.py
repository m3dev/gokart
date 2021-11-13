import unittest

import pandas as pd

import gokart


class TestPandasAssert(unittest.TestCase):
    def test_assert_frame_contents_equal(self):
        expected = pd.DataFrame(data=dict(f1=[1, 2, 3], f3=[111, 222, 333], f2=[4, 5, 6]), index=[0, 1, 2])
        resulted = pd.DataFrame(data=dict(f2=[5, 4, 6], f1=[2, 1, 3], f3=[222, 111, 333]), index=[1, 0, 2])

        gokart.testing.assert_frame_contents_equal(resulted, expected)

    def test_assert_frame_contents_equal_with_small_error(self):
        expected = pd.DataFrame(data=dict(f1=[1.0001, 2.0001, 3.0001], f3=[111, 222, 333], f2=[4, 5, 6]), index=[0, 1, 2])
        resulted = pd.DataFrame(data=dict(f2=[5, 4, 6], f1=[2.0002, 1.0002, 3.0002], f3=[222, 111, 333]), index=[1, 0, 2])

        gokart.testing.assert_frame_contents_equal(resulted, expected, atol=1e-1)

    def test_assert_frame_contents_equal_with_duplicated_columns(self):
        expected = pd.DataFrame(data=dict(f1=[1, 2, 3], f3=[111, 222, 333], f2=[4, 5, 6]), index=[0, 1, 2])
        expected.columns = ['f1', 'f1', 'f2']
        resulted = pd.DataFrame(data=dict(f2=[5, 4, 6], f1=[2, 1, 3], f3=[222, 111, 333]), index=[1, 0, 2])
        resulted.columns = ['f2', 'f1', 'f1']

        with self.assertRaises(AssertionError):
            gokart.testing.assert_frame_contents_equal(resulted, expected)

    def test_assert_frame_contents_equal_with_duplicated_indexes(self):
        expected = pd.DataFrame(data=dict(f1=[1, 2, 3], f3=[111, 222, 333], f2=[4, 5, 6]), index=[0, 1, 2])
        expected.index = [0, 1, 1]
        resulted = pd.DataFrame(data=dict(f2=[5, 4, 6], f1=[2, 1, 3], f3=[222, 111, 333]), index=[1, 0, 2])
        expected.index = [1, 0, 1]

        with self.assertRaises(AssertionError):
            gokart.testing.assert_frame_contents_equal(resulted, expected)
