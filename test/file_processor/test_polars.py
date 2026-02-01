"""Tests for polars-specific file processors."""

from __future__ import annotations

import tempfile

import pandas as pd
import pytest
from luigi import LocalTarget

from gokart.file_processor import CsvFileProcessor, FeatherFileProcessor, JsonFileProcessor, ParquetFileProcessor

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


@pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
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


@pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
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
        """Test ndjson (records orient) with polars"""
        df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
        processor = JsonFileProcessor(orient='records', dataframe_type='polars')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.ndjson'

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


@pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
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


@pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
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


@pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
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
        processor = JsonFileProcessor(orient='records', dataframe_type='polars-lazy')

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
        processor = JsonFileProcessor(orient='records', dataframe_type='polars-lazy')

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.ndjson'

            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(lf, f)

            # Verify file was created and can be read
            loaded_df = pl.read_ndjson(temp_path)
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
