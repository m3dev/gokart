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

    def __init__(self, sep=',', encoding: str = 'utf-8'):
        if not HAS_POLARS:
            raise ImportError("polars is required for return_type='polars'. Install with: pip install polars")
        self._sep = sep
        self._encoding = encoding
        super().__init__()

    def format(self):
        return TextFormat(encoding=self._encoding)

    def load(self, file):
        try:
            return pl.read_csv(file, separator=self._sep, encoding=self._encoding)
        except Exception as e:
            # Handle empty data gracefully
            if 'empty' in str(e).lower() or 'no data' in str(e).lower():
                return pl.DataFrame()
            raise

    def dump(self, obj, file):
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame, but {type(obj)} is passed.')
        obj.write_csv(file, separator=self._sep, include_header=True)


class JsonFileProcessorPolars(FileProcessor):
    """JSON file processor for polars DataFrames."""

    def __init__(self, orient: str | None = None):
        if not HAS_POLARS:
            raise ImportError("polars is required for return_type='polars'. Install with: pip install polars")
        self._orient = orient

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        try:
            if self._orient == 'records':
                return pl.read_ndjson(file)
            else:
                return pl.read_json(file)
        except Exception as e:
            # Handle empty files
            if 'empty' in str(e).lower() or 'no data' in str(e).lower():
                return pl.DataFrame()
            raise

    def dump(self, obj, file):
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame, but {type(obj)} is passed.')
        if self._orient == 'records':
            obj.write_ndjson(file)
        else:
            obj.write_json(file)


class ParquetFileProcessorPolars(FileProcessor):
    """Parquet file processor for polars DataFrames."""

    def __init__(self, engine='pyarrow', compression=None):
        if not HAS_POLARS:
            raise ImportError("polars is required for return_type='polars'. Install with: pip install polars")
        self._engine = engine  # Ignored for polars
        self._compression = compression
        super().__init__()

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        # polars.read_parquet can handle file paths or file-like objects
        if ObjectStorage.is_buffered_reader(file):
            return pl.read_parquet(file.name)
        else:
            return pl.read_parquet(BytesIO(file.read()))

    def dump(self, obj, file):
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame, but {type(obj)} is passed.')
        # polars write_parquet requires a file path
        obj.write_parquet(file.name, compression=self._compression)


class FeatherFileProcessorPolars(FileProcessor):
    """Feather file processor for polars DataFrames."""

    def __init__(self, store_index_in_feather: bool):
        if not HAS_POLARS:
            raise ImportError("polars is required for return_type='polars'. Install with: pip install polars")
        super().__init__()
        self._store_index_in_feather = store_index_in_feather  # Ignored for polars

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        # polars uses read_ipc for feather format
        if ObjectStorage.is_buffered_reader(file):
            return pl.read_ipc(file.name)
        else:
            return pl.read_ipc(BytesIO(file.read()))

    def dump(self, obj, file):
        if not isinstance(obj, pl.DataFrame):
            raise TypeError(f'requires pl.DataFrame, but {type(obj)} is passed.')
        # polars uses write_ipc for feather format
        # Note: store_index_in_feather is ignored for polars as it's pandas-specific
        obj.write_ipc(file.name)
