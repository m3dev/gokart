"""Pandas-specific file processor implementations."""

from __future__ import annotations

from io import BytesIO
from typing import Literal

import luigi
import luigi.format
import pandas as pd
from luigi.format import TextFormat

from gokart.file_processor.base import FileProcessor
from gokart.object_storage import ObjectStorage


class CsvFileProcessorPandas(FileProcessor):
    """CSV file processor for pandas DataFrames."""

    def __init__(self, sep=',', encoding: str = 'utf-8'):
        self._sep = sep
        self._encoding = encoding
        super().__init__()

    def format(self):
        return TextFormat(encoding=self._encoding)

    def load(self, file):
        try:
            return pd.read_csv(file, sep=self._sep, encoding=self._encoding)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def dump(self, obj, file):
        if not isinstance(obj, pd.DataFrame | pd.Series):
            raise TypeError(f'requires pd.DataFrame or pd.Series, but {type(obj)} is passed.')
        obj.to_csv(file, mode='wt', index=False, sep=self._sep, header=True, encoding=self._encoding)


_JsonOrient = Literal['split', 'records', 'index', 'table', 'columns', 'values']


class JsonFileProcessorPandas(FileProcessor):
    """JSON file processor for pandas DataFrames."""

    def __init__(self, orient: _JsonOrient | None = None):
        self._orient: _JsonOrient | None = orient

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        try:
            return pd.read_json(file, orient=self._orient, lines=True if self._orient == 'records' else False)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def dump(self, obj, file):
        if isinstance(obj, dict):
            obj = pd.DataFrame.from_dict(obj)
        if not isinstance(obj, pd.DataFrame | pd.Series):
            raise TypeError(f'requires pd.DataFrame or pd.Series or dict, but {type(obj)} is passed.')
        obj.to_json(file, orient=self._orient, lines=True if self._orient == 'records' else False)


class ParquetFileProcessorPandas(FileProcessor):
    """Parquet file processor for pandas DataFrames."""

    def __init__(self, engine: Literal['auto', 'pyarrow', 'fastparquet'] = 'pyarrow', compression=None):
        self._engine: Literal['auto', 'pyarrow', 'fastparquet'] = engine
        self._compression = compression
        super().__init__()

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        # FIXME(mamo3gr): enable streaming (chunked) read with S3.
        # pandas.read_parquet accepts file-like object
        # but file (luigi.contrib.s3.ReadableS3File) should have 'tell' method,
        # which is needed for pandas to read a file in chunks.
        if ObjectStorage.is_buffered_reader(file):
            return pd.read_parquet(file.name)
        else:
            return pd.read_parquet(BytesIO(file.read()))

    def dump(self, obj, file):
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f'requires pd.DataFrame, but {type(obj)} is passed.')
        # MEMO: to_parquet only supports a filepath as string (not a file handle)
        obj.to_parquet(file.name, index=False, engine=self._engine, compression=self._compression)


class FeatherFileProcessorPandas(FileProcessor):
    """Feather file processor for pandas DataFrames."""

    def __init__(self, store_index_in_feather: bool):
        super().__init__()
        self._store_index_in_feather = store_index_in_feather
        self.INDEX_COLUMN_PREFIX = '__feather_gokart_index__'

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        # FIXME(mamo3gr): enable streaming (chunked) read with S3.
        # pandas.read_feather accepts file-like object
        # but file (luigi.contrib.s3.ReadableS3File) should have 'tell' method,
        # which is needed for pandas to read a file in chunks.
        if ObjectStorage.is_buffered_reader(file):
            loaded_df = pd.read_feather(file.name)
        else:
            loaded_df = pd.read_feather(BytesIO(file.read()))

        if self._store_index_in_feather:
            if any(col.startswith(self.INDEX_COLUMN_PREFIX) for col in loaded_df.columns):
                index_columns = [col_name for col_name in loaded_df.columns[::-1] if col_name[: len(self.INDEX_COLUMN_PREFIX)] == self.INDEX_COLUMN_PREFIX]
                index_column = index_columns[0]
                index_name = index_column[len(self.INDEX_COLUMN_PREFIX) :]
                if index_name == 'None':
                    index_name = None
                loaded_df.index = pd.Index(loaded_df[index_column].values, name=index_name)
                loaded_df = loaded_df.drop(columns=[index_column])

        return loaded_df

    def dump(self, obj, file):
        if not isinstance(obj, pd.DataFrame):
            raise TypeError(f'requires pd.DataFrame, but {type(obj)} is passed.')

        dump_obj = obj.copy()

        if self._store_index_in_feather:
            index_column_name = f'{self.INDEX_COLUMN_PREFIX}{dump_obj.index.name}'
            assert index_column_name not in dump_obj.columns, (
                f'column name {index_column_name} already exists in dump_obj. \nConsider not saving index by setting store_index_in_feather=False.'
            )
            assert dump_obj.index.name != 'None', 'index name is "None", which is not allowed in gokart. Consider setting another index name.'

            dump_obj[index_column_name] = dump_obj.index
            dump_obj = dump_obj.reset_index(drop=True)

        # to_feather supports "binary" file-like object, but file variable is text
        dump_obj.to_feather(file.name)
