from __future__ import annotations

import tempfile

import pytest
from luigi import LocalTarget

from gokart.file_processor import PolarsCsvFileProcessor, PolarsFeatherFileProcessor, PolarsJsonFileProcessor

pl = pytest.importorskip('polars', reason='polars required')
pl_testing = pytest.importorskip('polars.testing', reason='polars required')


def test_dump_csv_with():
    df = pl.DataFrame({'あ': [1, 2, 3], 'い': [4, 5, 6]})
    processor = PolarsCsvFileProcessor()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f'{temp_dir}/temp.csv'

        local_target = LocalTarget(path=temp_path, format=processor.format())
        with local_target.open('w') as f:
            processor.dump(df, f)

        # read with utf-8 to check if the file is dumped with utf8
        loaded_df = pl.read_csv(temp_path)
        pl_testing.assert_frame_equal(df, loaded_df)


def test_load_csv():
    df = pl.DataFrame({'あ': [1, 2, 3], 'い': [4, 5, 6]})
    processor = PolarsCsvFileProcessor()

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f'{temp_dir}/temp.csv'
        df.write_csv(temp_path, include_header=True)

        local_target = LocalTarget(path=temp_path, format=processor.format())
        with local_target.open('r') as f:
            # read with utf-8 to check if the file is dumped with utf8
            loaded_df = processor.load(f)
            pl_testing.assert_frame_equal(df, loaded_df)


@pytest.mark.parametrize(
    'orient,input_data,expected_json',
    [
        pytest.param(
            None,
            pl.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
            '[{"A":1,"B":4},{"A":2,"B":5},{"A":3,"B":6}]',
            id='With Default Orient for DataFrame',
        ),
        pytest.param(
            'records',
            pl.DataFrame({'A': [1, 2, 3], 'B': [4, 5, 6]}),
            '{"A":1,"B":4}\n{"A":2,"B":5}\n{"A":3,"B":6}\n',
            id='With Records Orient for DataFrame',
        ),
        pytest.param(None, {'A': [1, 2, 3], 'B': [4, 5, 6]}, '[{"A":1,"B":4},{"A":2,"B":5},{"A":3,"B":6}]', id='With Default Orient for Dict'),
        pytest.param('records', {'A': [1, 2, 3], 'B': [4, 5, 6]}, '{"A":1,"B":4}\n{"A":2,"B":5}\n{"A":3,"B":6}\n', id='With Records Orient for Dict'),
        pytest.param(None, {}, '[]', id='With Default Orient for Empty Dict'),
        pytest.param('records', {}, '', id='With Records Orient for Empty Dict'),
    ],
)
def test_dump_and_load_json(orient, input_data, expected_json):
    processor = PolarsJsonFileProcessor(orient=orient)

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

    df_input = pl.DataFrame(input_data)
    pl_testing.assert_frame_equal(df_input, loaded_df)


def test_feather_should_return_same_dataframe():
    df = pl.DataFrame({'a': [1]})
    # TODO: currently we set store_index_in_feather True but it is ignored
    processor = PolarsFeatherFileProcessor(store_index_in_feather=True)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = f'{temp_dir}/temp.feather'

        local_target = LocalTarget(path=temp_path, format=processor.format())
        with local_target.open('w') as f:
            processor.dump(df, f)

        with local_target.open('r') as f:
            loaded_df = processor.load(f)

        pl_testing.assert_frame_equal(df, loaded_df)
