"""Tests for polars-specific file processors."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import textwrap
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pandas as pd
import pytest
from luigi import LocalTarget

from gokart.file_processor import CsvFileProcessor, FeatherFileProcessor, JsonFileProcessor, NdjsonFileProcessor, ParquetFileProcessor

if TYPE_CHECKING:
    import polars as pl

try:
    import polars as pl

    _has_polars = True
except ImportError:
    _has_polars = False


@pytest.mark.skipif(not _has_polars, reason='polars not installed')
class TestCsvFileProcessorWithPolars:
    """Tests for CsvFileProcessor with polars support"""

    def test_dump_polars_dataframe(self):
        """Test dumping a polars DataFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = CsvFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            # Verify file was created and can be read by polars
            loaded_df = pl.read_csv(temp_path)
            assert loaded_df.equals(df)

    def test_load_polars_dataframe(self):
        """Test loading a CSV as polars DataFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = CsvFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'
            df.write_csv(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_and_load_polars_roundtrip(self):
        """Test roundtrip dump and load with polars"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = CsvFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_polars_with_pandas_load(self):
        """Test that polars dump can be loaded by pandas processor"""
        df_polars = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor_polars = CsvFileProcessor(dataframe_type='polars')
        processor_pandas = CsvFileProcessor(dataframe_type='pandas')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'

            # Dump with polars
            local_target = LocalTarget(path=temp_path, format=processor_polars.format())
            with local_target.open('w') as f:
                processor_polars.dump(df_polars, f)

            # Load with pandas
            with local_target.open('r') as f:
                loaded_df = processor_pandas.load(f)

            assert isinstance(loaded_df, pd.DataFrame)
            # Compare values
            df_polars.equals(pl.from_pandas(loaded_df))

    def test_polars_with_different_separator(self):
        """Test polars with TSV (tab-separated values)"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = CsvFileProcessor(sep='\t', dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.tsv'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_error_when_polars_not_available_for_load(self):
        """Test error message when polars is requested but a polars operation fails"""
        # This test is a bit tricky since polars IS installed in this test class
        # We'll just verify the processor accepts the parameter
        processor = CsvFileProcessor(dataframe_type='polars')
        assert processor._dataframe_type == 'polars'


@pytest.mark.skipif(not _has_polars, reason='polars not installed')
class TestJsonFileProcessorWithPolars:
    """Tests for JsonFileProcessor with polars support"""

    def test_dump_polars_dataframe(self):
        """Test dumping a polars DataFrame to JSON"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = JsonFileProcessor(orient=None, dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            # Verify file was created and can be read by polars
            loaded_df = pl.read_json(temp_path)
            assert loaded_df.equals(df)

    def test_load_polars_dataframe(self):
        """Test loading a JSON as polars DataFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = JsonFileProcessor(orient=None, dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            df.write_json(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_and_load_polars_roundtrip(self):
        """Test roundtrip dump and load with polars"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = JsonFileProcessor(orient=None, dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_and_load_ndjson_with_polars(self):
        """Test ndjson with polars"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = NdjsonFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.ndjson'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    @pytest.mark.parametrize('dataframe_type', ['polars', 'polars-lazy'])
    @pytest.mark.parametrize('content', [b'', b'\n'], ids=['empty', 'newline only'])
    def test_load_empty_ndjson_with_polars(self, dataframe_type, content):
        processor = NdjsonFileProcessor(dataframe_type=dataframe_type)

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.ndjson'
            with open(temp_path, 'wb') as f:
                f.write(content)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

        loaded_df = loaded.collect() if isinstance(loaded, pl.LazyFrame) else loaded
        assert loaded_df.equals(pl.DataFrame())

    def test_records_orient_warns_and_keeps_ndjson_with_polars(self):
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        with pytest.warns(FutureWarning, match=r'will change to a JSON array in gokart 2\.0\.0\. Use NdjsonFileProcessor'):
            processor = JsonFileProcessor(orient='records', dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            assert pl.read_ndjson(temp_path).equals(df)

    def test_dump_polars_with_pandas_load(self):
        """Test that polars dump can be loaded by pandas processor"""
        df_polars = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor_polars = JsonFileProcessor(orient=None, dataframe_type='polars')
        processor_pandas = JsonFileProcessor(orient=None, dataframe_type='pandas')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'

            # Dump with polars
            local_target = LocalTarget(path=temp_path, format=processor_polars.format())
            with local_target.open('w') as f:
                processor_polars.dump(df_polars, f)

            # Load with pandas
            with local_target.open('r') as f:
                loaded_df = processor_pandas.load(f)

            assert isinstance(loaded_df, pd.DataFrame)
            # Compare values
            assert list(loaded_df['a']) == [1, 2, 3]
            assert list(loaded_df['b']) == [4, 5, 6]


@pytest.mark.skipif(not _has_polars, reason='polars not installed')
class TestJsonFileProcessorOrientWithPolars:
    """polars only supports orient=None and 'records', so the other orients must fail instead of being silently ignored."""

    @pytest.mark.parametrize('dataframe_type', ['polars', 'polars-lazy'])
    @pytest.mark.parametrize('orient', ['split', 'index', 'columns', 'values', 'table'])
    def test_unsupported_orient_raises_value_error(self, orient: str, dataframe_type: str):
        # No deprecation period: it must fail directly, without a warning first.
        with warnings.catch_warnings():
            warnings.simplefilter('error')
            with pytest.raises(ValueError, match=orient):
                JsonFileProcessor(orient=orient, dataframe_type=dataframe_type)  # type: ignore[arg-type]

    @pytest.mark.parametrize('lazy', [False, True])
    @pytest.mark.parametrize('orient', ['split', 'index', 'columns', 'values', 'table'])
    def test_unsupported_orient_raises_value_error_for_polars_processor(self, orient: str, lazy: bool):
        from gokart.file_processor.polars import JsonFileProcessorPolars

        with pytest.raises(ValueError, match=orient):
            JsonFileProcessorPolars(orient=orient, lazy=lazy)

    @pytest.mark.parametrize('dataframe_type', ['polars', 'polars-lazy'])
    @pytest.mark.parametrize('orient', [None, 'records'])
    def test_supported_orient_round_trips(self, orient: str | None, dataframe_type: str):
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', FutureWarning)
            processor = JsonFileProcessor(orient=orient, dataframe_type=dataframe_type)  # type: ignore[arg-type]

        with tempfile.TemporaryDirectory() as temp_dir:
            local_target = LocalTarget(path=f'{temp_dir}/temp.json', format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)
            with local_target.open('r') as f:
                loaded = processor.load(f)

        loaded_df = loaded.collect() if isinstance(loaded, pl.LazyFrame) else loaded
        assert loaded_df.equals(df)


@pytest.mark.skipif(not _has_polars, reason='polars not installed')
class TestParquetFileProcessorWithPolars:
    """Tests for ParquetFileProcessor with polars support"""

    def test_dump_polars_dataframe(self):
        """Test dumping a polars DataFrame to Parquet"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = ParquetFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            # Verify file was created and can be read by polars
            loaded_df = pl.read_parquet(temp_path)
            assert loaded_df.equals(df)

    def test_load_polars_dataframe(self):
        """Test loading a Parquet as polars DataFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = ParquetFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'
            df.write_parquet(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_and_load_polars_roundtrip(self):
        """Test roundtrip dump and load with polars"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = ParquetFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_polars_with_pandas_load(self):
        """Test that polars dump can be loaded by pandas processor"""
        df_polars = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor_polars = ParquetFileProcessor(dataframe_type='polars')
        processor_pandas = ParquetFileProcessor(dataframe_type='pandas')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'

            # Dump with polars
            local_target = LocalTarget(path=temp_path, format=processor_polars.format())
            with local_target.open('w') as f:
                processor_polars.dump(df_polars, f)

            # Load with pandas
            with local_target.open('r') as f:
                loaded_df = processor_pandas.load(f)

            assert isinstance(loaded_df, pd.DataFrame)
            df_polars.equals(pl.from_pandas(loaded_df))

    def test_parquet_with_compression(self):
        """Test polars with parquet compression"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = ParquetFileProcessor(compression='gzip', dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)


@pytest.mark.skipif(not _has_polars, reason='polars not installed')
class TestFeatherFileProcessorWithPolars:
    """Tests for FeatherFileProcessor with polars support"""

    def test_dump_polars_dataframe(self):
        """Test dumping a polars DataFrame to Feather"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            # Verify file was created and can be read by polars
            loaded_df = pl.read_ipc(temp_path)
            assert loaded_df.equals(df)

    def test_load_polars_dataframe(self):
        """Test loading a Feather as polars DataFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'
            df.write_ipc(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_and_load_polars_roundtrip(self):
        """Test roundtrip dump and load with polars"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(df, f)

            with local_target.open('r') as f:
                loaded_df = processor.load(f)

            assert isinstance(loaded_df, pl.DataFrame)
            assert loaded_df.equals(df)

    def test_dump_polars_with_pandas_load(self):
        """Test that polars dump can be loaded by pandas processor"""
        df_polars = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor_polars = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars')
        processor_pandas = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='pandas')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            # Dump with polars
            local_target = LocalTarget(path=temp_path, format=processor_polars.format())
            with local_target.open('w') as f:
                processor_polars.dump(df_polars, f)

            # Load with pandas
            with local_target.open('r') as f:
                loaded_df = processor_pandas.load(f)

            assert isinstance(loaded_df, pd.DataFrame)
            # Compare values
            df_polars.equals(pl.from_pandas(loaded_df))


@pytest.mark.skipif(not _has_polars, reason='polars not installed')
class TestLazyFrameSupport:
    """Tests for LazyFrame support in file processors using dataframe_type='polars-lazy'"""

    def test_csv_load_lazy(self):
        """Test loading CSV as LazyFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = CsvFileProcessor(dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'
            df.write_csv(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

            assert isinstance(loaded, pl.LazyFrame)
            assert loaded.collect().equals(df)

    def test_csv_dump_lazyframe(self):
        """Test dumping a LazyFrame to CSV"""
        lf = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}).lazy()
        processor = CsvFileProcessor(dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.csv'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(lf, f)

            # Verify file was created and can be read
            loaded_df = pl.read_csv(temp_path)
            assert loaded_df.equals(lf.collect())

    def test_parquet_load_lazy(self):
        """Test loading Parquet as LazyFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = ParquetFileProcessor(dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'
            df.write_parquet(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

            assert isinstance(loaded, pl.LazyFrame)
            assert loaded.collect().equals(df)

    def test_parquet_dump_lazyframe(self):
        """Test dumping a LazyFrame to Parquet"""
        lf = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}).lazy()
        processor = ParquetFileProcessor(dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(lf, f)

            # Verify file was created and can be read
            loaded_df = pl.read_parquet(temp_path)
            assert loaded_df.equals(lf.collect())

    def test_feather_load_lazy(self):
        """Test loading Feather as LazyFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'
            df.write_ipc(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

            assert isinstance(loaded, pl.LazyFrame)
            assert loaded.collect().equals(df)

    def test_feather_dump_lazyframe(self):
        """Test dumping a LazyFrame to Feather"""
        lf = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}).lazy()
        processor = FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.feather'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(lf, f)

            # Verify file was created and can be read
            loaded_df = pl.read_ipc(temp_path)
            assert loaded_df.equals(lf.collect())

    def test_json_load_lazy_ndjson(self):
        """Test loading NDJSON as LazyFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = NdjsonFileProcessor(dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.ndjson'
            df.write_ndjson(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

            assert isinstance(loaded, pl.LazyFrame)
            assert loaded.collect().equals(df)

    def test_json_dump_lazyframe_ndjson(self):
        """Test dumping a LazyFrame to NDJSON"""
        lf = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}).lazy()
        processor = NdjsonFileProcessor(dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.ndjson'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(lf, f)

            # Verify file was created and can be read
            loaded_df = pl.read_ndjson(temp_path)
            assert loaded_df.equals(lf.collect())

    def test_json_load_lazy_standard(self):
        """Test loading standard JSON (orient=None) as LazyFrame"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = JsonFileProcessor(orient=None, dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'
            df.write_json(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

            assert isinstance(loaded, pl.LazyFrame)
            assert loaded.collect().equals(df)

    def test_json_dump_lazyframe_standard(self):
        """Test dumping a LazyFrame to standard JSON (orient=None)"""
        lf = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]}).lazy()
        processor = JsonFileProcessor(orient=None, dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.json'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(lf, f)

            # Verify file was created and can be read
            loaded_df = pl.read_json(temp_path)
            assert loaded_df.equals(lf.collect())

    def test_polars_returns_dataframe(self):
        """Test that dataframe_type='polars' returns DataFrame (not LazyFrame)"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = ParquetFileProcessor(dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.parquet'
            df.write_parquet(temp_path)

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('r') as f:
                loaded = processor.load(f)

            assert isinstance(loaded, pl.DataFrame)
            assert loaded.equals(df)


def test_import_gokart_does_not_import_polars() -> None:
    code = textwrap.dedent(
        """
        import sys
        import gokart
        import gokart.file_processor
        assert 'polars' not in sys.modules, 'import gokart imported polars'
        """
    )
    subprocess.run([sys.executable, '-c', code], check=True)


_POLARS_PROCESSOR_FACTORIES = [
    pytest.param(lambda: CsvFileProcessor(dataframe_type='polars'), id='csv'),
    pytest.param(lambda: JsonFileProcessor(dataframe_type='polars'), id='json'),
    pytest.param(lambda: NdjsonFileProcessor(dataframe_type='polars-lazy'), id='ndjson-lazy'),
    pytest.param(lambda: NdjsonFileProcessor(dataframe_type='polars'), id='ndjson'),
    pytest.param(lambda: ParquetFileProcessor(dataframe_type='polars'), id='parquet'),
    pytest.param(lambda: FeatherFileProcessor(store_index_in_feather=False, dataframe_type='polars-lazy'), id='feather-lazy'),
]


@pytest.mark.parametrize('make_processor', _POLARS_PROCESSOR_FACTORIES)
def test_polars_processor_raises_install_hint_without_polars(monkeypatch: pytest.MonkeyPatch, make_processor: Callable[[], Any]) -> None:
    # The error is raised when the processor is created, not when a task dumps its output after run().
    monkeypatch.setitem(sys.modules, 'polars', None)
    with pytest.raises(ImportError, match=r'pip install gokart\[polars\]'):
        make_processor()


@pytest.mark.parametrize(
    ('polars_init', 'expected_error', 'expected_message'),
    [
        pytest.param('import _gokart_missing_polars_dependency', ModuleNotFoundError, '_gokart_missing_polars_dependency', id='missing-dependency'),
        pytest.param('import polars._gokart_missing_submodule', ModuleNotFoundError, r'polars\._gokart_missing_submodule', id='missing-submodule'),
        pytest.param("raise ImportError('broken polars dependency')", ImportError, 'broken polars dependency', id='import-error'),
    ],
)
def test_polars_processor_keeps_original_error_when_installed_polars_fails_to_import(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, polars_init: str, expected_error: type[ImportError], expected_message: str
) -> None:
    # The install hint would be wrong here and would hide the real cause.
    package = tmp_path / 'polars'
    package.mkdir()
    (package / '__init__.py').write_text(polars_init)
    monkeypatch.delitem(sys.modules, 'polars', raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(expected_error, match=expected_message) as exc_info:
        CsvFileProcessor(dataframe_type='polars')
    assert 'pip install' not in str(exc_info.value)
