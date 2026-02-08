"""Polars-specific file processor implementations."""

from __future__ import annotations

from io import BytesIO

import luigi
import luigi.format
from luigi.format import TextFormat

from gokart.file_processor.base import FileProcessor
from gokart.object_storage import ObjectStorage

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False
    pl = None  # type: ignore


class CsvFileProcessorPolars(FileProcessor):
    """CSV file processor for polars DataFrames."""

    def __init__(self, sep=',', encoding: str = 'utf-8', lazy: bool = False):
        if not HAS_POLARS:
            raise ImportError("polars is required for polars-based dataframe types ('polars' or 'polars-lazy'). Install with: pip install polars")
        self._sep = sep
        self._encoding = encoding
        self._lazy = lazy
        super().__init__()

    def format(self):
        return TextFormat(encoding=self._encoding)

    def load(self, file):
        try:
            if self._lazy:
                # scan_csv requires a file path, not a file object
                # Also, scan_csv uses 'utf8' instead of 'utf-8'
                encoding = 'utf8' if self._encoding == 'utf-8' else self._encoding
                return pl.scan_csv(file.name, separator=self._sep, encoding=encoding)
            return pl.read_csv(file, separator=self._sep, encoding=self._encoding)
        except Exception as e:
            # Handle empty data gracefully
            if 'empty' in str(e).lower() or 'no data' in str(e).lower():
                return pl.LazyFrame() if self._lazy else pl.DataFrame()
            raise

    def dump(self, obj, file):
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame or pl.LazyFrame, but {type(obj)} is passed.')
        obj.write_csv(file, separator=self._sep, include_header=True)


class JsonFileProcessorPolars(FileProcessor):
    """JSON file processor for polars DataFrames."""

    def __init__(self, orient: str | None = None, lazy: bool = False):
        if not HAS_POLARS:
            raise ImportError("polars is required for polars-based dataframe types ('polars' or 'polars-lazy'). Install with: pip install polars")
        self._orient = orient
        self._lazy = lazy

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        try:
            if self._orient == 'records':
                if self._lazy:
                    return pl.scan_ndjson(file)
                return pl.read_ndjson(file)
            else:
                # polars doesn't have scan_json, so we read and convert if lazy
                df = pl.read_json(file)
                return df.lazy() if self._lazy else df
        except Exception as e:
            # Handle empty files
            if 'empty' in str(e).lower() or 'no data' in str(e).lower():
                return pl.LazyFrame() if self._lazy else pl.DataFrame()
            raise

    def dump(self, obj, file):
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame or pl.LazyFrame, but {type(obj)} is passed.')
        if self._orient == 'records':
            obj.write_ndjson(file)
        else:
            obj.write_json(file)


class ParquetFileProcessorPolars(FileProcessor):
    """Parquet file processor for polars DataFrames."""

    def __init__(self, engine='pyarrow', compression=None, lazy: bool = False):
        if not HAS_POLARS:
            raise ImportError("polars is required for polars-based dataframe types ('polars' or 'polars-lazy'). Install with: pip install polars")
        self._engine = engine  # Ignored for polars
        self._compression = compression
        self._lazy = lazy
        super().__init__()

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        # polars.read_parquet can handle file paths or file-like objects
        if ObjectStorage.is_buffered_reader(file):
            if self._lazy:
                return pl.scan_parquet(file.name)
            return pl.read_parquet(file.name)
        else:
            data = BytesIO(file.read())
            if self._lazy:
                # scan_parquet doesn't work with BytesIO, so read and convert
                return pl.read_parquet(data).lazy()
            return pl.read_parquet(data)

    def dump(self, obj, file):
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame or pl.LazyFrame, but {type(obj)} is passed.')
        # polars write_parquet requires a file path
        obj.write_parquet(file.name, compression=self._compression)


class FeatherFileProcessorPolars(FileProcessor):
    """Feather file processor for polars DataFrames."""

    def __init__(self, store_index_in_feather: bool, lazy: bool = False):
        if not HAS_POLARS:
            raise ImportError("polars is required for polars-based dataframe types ('polars' or 'polars-lazy'). Install with: pip install polars")
        super().__init__()
        self._store_index_in_feather = store_index_in_feather  # Ignored for polars
        self._lazy = lazy

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        # polars uses read_ipc for feather format
        if ObjectStorage.is_buffered_reader(file):
            if self._lazy:
                return pl.scan_ipc(file.name)
            return pl.read_ipc(file.name)
        else:
            data = BytesIO(file.read())
            if self._lazy:
                # scan_ipc doesn't work with BytesIO, so read and convert
                return pl.read_ipc(data).lazy()
            return pl.read_ipc(data)

    def dump(self, obj, file):
        if isinstance(obj, pl.LazyFrame):
            obj = obj.collect()
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame or pl.LazyFrame, but {type(obj)} is passed.')
        # polars uses write_ipc for feather format
        # Note: store_index_in_feather is ignored for polars as it's pandas-specific
        obj.write_ipc(file.name)
