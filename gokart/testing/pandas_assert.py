import pandas as pd


def assert_frame_contents_equal(actual: pd.DataFrame, expected: pd.DataFrame, **kwargs):
    """
    Assert that two DataFrames are equal.
    This function is mostly same as pandas.testing.assert_frame_equal, however
    - this fuction ignores the order of index and columns.
    - this function fails when duplicated index or columns are found.

    Parameters
    ----------
    - actual, expected: pd.DataFrame
        DataFrames to be compared.
    - kwargs: Any
        Parameters passed to pandas.testing.assert_frame_equal.
    """
    assert isinstance(actual, pd.DataFrame), 'actual is not a DataFrame'
    assert isinstance(expected, pd.DataFrame), 'expected is not a DataFrame'

    assert actual.index.is_unique, 'actual index is not unique'
    assert expected.index.is_unique, 'expected index is not unique'
    assert actual.columns.is_unique, 'actual columns is not unique'
    assert expected.columns.is_unique, 'expected columns is not unique'

    assert set(actual.columns) == set(expected.columns), 'columns are not equal'
    assert set(actual.index) == set(expected.index), 'indexes are not equal'

    expected_reindexed = expected.reindex(actual.index)[actual.columns]
    pd.testing.assert_frame_equal(actual, expected_reindexed, **kwargs)
