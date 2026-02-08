"""File processor module with support for multiple DataFrame backends."""

from __future__ import annotations

import os

# Export common processors and types from base
from gokart.file_processor.base import (
    BinaryFileProcessor,
    DataFrameType,
    FileProcessor,
    GzipFileProcessor,
    NpzFileProcessor,
    PickleFileProcessor,
    TextFileProcessor,
    XmlFileProcessor,
)

# Import backend-specific implementations
from gokart.file_processor.pandas import (
    CsvFileProcessorPandas,
    FeatherFileProcessorPandas,
    JsonFileProcessorPandas,
    ParquetFileProcessorPandas,
)
from gokart.file_processor.polars import (
    CsvFileProcessorPolars,
    FeatherFileProcessorPolars,
    JsonFileProcessorPolars,
    ParquetFileProcessorPolars,
)


class CsvFileProcessor(FileProcessor):
    """CSV file processor with automatic backend selection based on dataframe_type."""

    def __init__(self, sep=',', encoding: str = 'utf-8', dataframe_type: DataFrameType = 'pandas'):
        """
        CSV file processor with support for both pandas and polars DataFrames.

        Args:
            sep: CSV delimiter (default: ',')
            encoding: File encoding (default: 'utf-8')
            dataframe_type: DataFrame library to use for load() - 'pandas', 'polars', or 'polars-lazy' (default: 'pandas')
        """
        self._sep = sep
        self._encoding = encoding
        self._dataframe_type = dataframe_type  # Store for tests

        if dataframe_type == 'polars-lazy':
            self._impl: FileProcessor = CsvFileProcessorPolars(sep=sep, encoding=encoding, lazy=True)
        elif dataframe_type == 'polars':
            self._impl = CsvFileProcessorPolars(sep=sep, encoding=encoding, lazy=False)
        else:
            self._impl = CsvFileProcessorPandas(sep=sep, encoding=encoding)

    def format(self):
        return self._impl.format()

    def load(self, file):
        return self._impl.load(file)

    def dump(self, obj, file):
        return self._impl.dump(obj, file)


class JsonFileProcessor(FileProcessor):
    """JSON file processor with automatic backend selection based on dataframe_type."""

    def __init__(self, orient: str | None = None, dataframe_type: DataFrameType = 'pandas'):
        """
        JSON file processor with support for both pandas and polars DataFrames.

        Args:
            orient: JSON orientation. 'records' for newline-delimited JSON.
            dataframe_type: DataFrame library to use for load() - 'pandas', 'polars', or 'polars-lazy' (default: 'pandas')
        """
        self._orient = orient
        self._dataframe_type = dataframe_type  # Store for tests

        if dataframe_type == 'polars-lazy':
            self._impl: FileProcessor = JsonFileProcessorPolars(orient=orient, lazy=True)
        elif dataframe_type == 'polars':
            self._impl = JsonFileProcessorPolars(orient=orient, lazy=False)
        else:
            self._impl = JsonFileProcessorPandas(orient=orient)

    def format(self):
        return self._impl.format()

    def load(self, file):
        return self._impl.load(file)

    def dump(self, obj, file):
        return self._impl.dump(obj, file)


class ParquetFileProcessor(FileProcessor):
    """Parquet file processor with automatic backend selection based on dataframe_type."""

    def __init__(self, engine='pyarrow', compression=None, dataframe_type: DataFrameType = 'pandas'):
        """
        Parquet file processor with support for both pandas and polars DataFrames.

        Args:
            engine: Parquet engine (pandas-specific, ignored for polars).
            compression: Compression type.
            dataframe_type: DataFrame library to use for load() - 'pandas', 'polars', or 'polars-lazy' (default: 'pandas')
        """
        self._engine = engine
        self._compression = compression
        self._dataframe_type = dataframe_type  # Store for tests

        if dataframe_type == 'polars-lazy':
            self._impl: FileProcessor = ParquetFileProcessorPolars(engine=engine, compression=compression, lazy=True)
        elif dataframe_type == 'polars':
            self._impl = ParquetFileProcessorPolars(engine=engine, compression=compression, lazy=False)
        else:
            self._impl = ParquetFileProcessorPandas(engine=engine, compression=compression)

    def format(self):
        return self._impl.format()

    def load(self, file):
        return self._impl.load(file)

    def dump(self, obj, file):
        # Use the configured implementation (pandas by default)
        return self._impl.dump(obj, file)


class FeatherFileProcessor(FileProcessor):
    """Feather file processor with automatic backend selection based on dataframe_type."""

    def __init__(self, store_index_in_feather: bool, dataframe_type: DataFrameType = 'pandas'):
        """
        Feather file processor with support for both pandas and polars DataFrames.

        Args:
            store_index_in_feather: Whether to store pandas index (pandas-only feature).
            dataframe_type: DataFrame library to use for load() - 'pandas', 'polars', or 'polars-lazy' (default: 'pandas')
        """
        self._store_index_in_feather = store_index_in_feather
        self._dataframe_type = dataframe_type  # Store for tests

        if dataframe_type == 'polars-lazy':
            self._impl: FileProcessor = FeatherFileProcessorPolars(store_index_in_feather=store_index_in_feather, lazy=True)
        elif dataframe_type == 'polars':
            self._impl = FeatherFileProcessorPolars(store_index_in_feather=store_index_in_feather, lazy=False)
        else:
            self._impl = FeatherFileProcessorPandas(store_index_in_feather=store_index_in_feather)

    def format(self):
        return self._impl.format()

    def load(self, file):
        return self._impl.load(file)

    def dump(self, obj, file):
        # Use the configured implementation (pandas by default)
        return self._impl.dump(obj, file)


def make_file_processor(file_path: str, store_index_in_feather: bool) -> FileProcessor:
    """Create a file processor based on file extension with default parameters."""
    extension2processor = {
        '.txt': TextFileProcessor(),
        '.ini': TextFileProcessor(),
        '.csv': CsvFileProcessor(sep=','),
        '.tsv': CsvFileProcessor(sep='\t'),
        '.pkl': PickleFileProcessor(),
        '.gz': GzipFileProcessor(),
        '.json': JsonFileProcessor(),
        '.ndjson': JsonFileProcessor(orient='records'),
        '.xml': XmlFileProcessor(),
        '.npz': NpzFileProcessor(),
        '.parquet': ParquetFileProcessor(compression='gzip'),
        '.feather': FeatherFileProcessor(store_index_in_feather=store_index_in_feather),
        '.png': BinaryFileProcessor(),
        '.jpg': BinaryFileProcessor(),
    }

    extension = os.path.splitext(file_path)[1]
    assert extension in extension2processor, f'{extension} is not supported. The supported extensions are {list(extension2processor.keys())}.'
    return extension2processor[extension]


__all__ = [
    # Base classes and types
    'FileProcessor',
    'DataFrameType',
    # Common processors
    'BinaryFileProcessor',
    'PickleFileProcessor',
    'TextFileProcessor',
    'GzipFileProcessor',
    'XmlFileProcessor',
    'NpzFileProcessor',
    # DataFrame processors (with factory pattern)
    'CsvFileProcessor',
    'JsonFileProcessor',
    'ParquetFileProcessor',
    'FeatherFileProcessor',
    # Utility functions
    'make_file_processor',
]
