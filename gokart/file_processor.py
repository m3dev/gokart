from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from abc import abstractmethod
from io import BytesIO
from logging import getLogger

import dill
import luigi
import luigi.contrib.s3
import luigi.format
import pandas as pd
import numpy as np
from luigi.format import TextFormat

from gokart.object_storage import ObjectStorage
from gokart.utils import load_dill_with_pandas_backward_compatibility

logger = getLogger(__name__)


try:
    import polars as pl

    if os.getenv('GOKART_DATAFRAME_FRAMEWORK_POLARS_ENABLED') == 'true':
        DATAFRAME_FRAMEWORK = 'polars'
    else:
        raise ValueError('GOKART_DATAFRAME_FRAMEWORK is not set. Use pandas as dataframe framework.')
except (ImportError, ValueError):
    DATAFRAME_FRAMEWORK = 'pandas'


class FileProcessor:
    @abstractmethod
    def format(self):
        pass

    @abstractmethod
    def load(self, file):
        pass

    @abstractmethod
    def dump(self, obj, file):
        pass


class BinaryFileProcessor(FileProcessor):
    """
    Pass bytes to this processor

    ```
    figure_binary = io.BytesIO()
    plt.savefig(figure_binary)
    figure_binary.seek(0)
    BinaryFileProcessor().dump(figure_binary.read())
    ```
    """

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        return file.read()

    def dump(self, obj, file):
        file.write(obj)


class _ChunkedLargeFileReader:
    def __init__(self, file) -> None:
        self._file = file

    def __getattr__(self, item):
        return getattr(self._file, item)

    def read(self, n):
        if n >= (1 << 31):
            logger.info(f'reading a large file with total_bytes={n}.')
            buffer = bytearray(n)
            idx = 0
            while idx < n:
                batch_size = min(n - idx, 1 << 31 - 1)
                logger.info(f'reading bytes [{idx}, {idx + batch_size})...')
                buffer[idx : idx + batch_size] = self._file.read(batch_size)
                idx += batch_size
            logger.info('done.')
            return buffer
        return self._file.read(n)


class PickleFileProcessor(FileProcessor):
    def format(self):
        return luigi.format.Nop

    def load(self, file):
        if not file.seekable():
            # load_dill_with_pandas_backward_compatibility() requires file with seek() and readlines() implemented.
            # Therefore, we need to wrap with BytesIO which makes file seekable and readlinesable.
            # For example, ReadableS3File is not a seekable file.
            return load_dill_with_pandas_backward_compatibility(BytesIO(file.read()))
        return load_dill_with_pandas_backward_compatibility(_ChunkedLargeFileReader(file))

    def dump(self, obj, file):
        self._write(dill.dumps(obj, protocol=4), file)

    @staticmethod
    def _write(buffer, file):
        n = len(buffer)
        idx = 0
        while idx < n:
            logger.info(f'writing a file with total_bytes={n}...')
            batch_size = min(n - idx, 1 << 31 - 1)
            logger.info(f'writing bytes [{idx}, {idx + batch_size})')
            file.write(buffer[idx : idx + batch_size])
            idx += batch_size
        logger.info('done')


class TextFileProcessor(FileProcessor):
    def format(self):
        return None

    def load(self, file):
        return [s.rstrip() for s in file.readlines()]

    def dump(self, obj, file):
        if isinstance(obj, list):
            for x in obj:
                file.write(str(x) + '\n')
        else:
            file.write(str(obj))


class CsvFileProcessor(FileProcessor):
    def __init__(self, sep=',', encoding: str = 'utf-8'):
        self._sep = sep
        self._encoding = encoding
        super().__init__()

    def format(self):
        return TextFormat(encoding=self._encoding)

    def load(self, file): ...

    def dump(self, obj, file): ...


class PolarsCsvFileProcessor(CsvFileProcessor):
    def load(self, file):
        try:
            return pl.read_csv(file, separator=self._sep, encoding=self._encoding)
        except pl.exceptions.NoDataError:
            return pl.DataFrame()

    def dump(self, obj, file):
        assert isinstance(obj, (pl.DataFrame, pl.Series)), f'requires pl.DataFrame or pl.Series, but {type(obj)} is passed.'
        obj.write_csv(file, separator=self._sep, include_header=True)


class PandasCsvFileProcessor(CsvFileProcessor):
    def load(self, file):
        try:
            return pd.read_csv(file, sep=self._sep, encoding=self._encoding)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def dump(self, obj, file):
        assert isinstance(obj, (pd.DataFrame, pd.Series)), f'requires pd.DataFrame or pd.Series, but {type(obj)} is passed.'
        obj.to_csv(file, mode='wt', index=False, sep=self._sep, header=True, encoding=self._encoding)


class GzipFileProcessor(FileProcessor):
    def format(self):
        return luigi.format.Gzip

    def load(self, file):
        return [s.rstrip().decode() for s in file.readlines()]

    def dump(self, obj, file):
        if isinstance(obj, list):
            for x in obj:
                file.write((str(x) + '\n').encode())
        else:
            file.write(str(obj).encode())


class JsonFileProcessor(FileProcessor):
    def __init__(self, orient: str | None = None):
        self._orient = orient

    def format(self):
        return luigi.format.Nop

    def load(self, file): ...

    def dump(self, obj, file): ...


class PolarsJsonFileProcessor(JsonFileProcessor):
    def load(self, file):
        try:
            if self._orient == 'records':
                return pl.read_ndjson(file)
            return pl.read_json(file)
        except pl.exceptions.ComputeError:
            return pl.DataFrame()

    def dump(self, obj, file):
        assert isinstance(obj, pl.DataFrame) or isinstance(obj, pl.Series) or isinstance(obj, dict), (
            f'requires pl.DataFrame or pl.Series or dict, but {type(obj)} is passed.'
        )
        if isinstance(obj, dict):
            obj = pl.from_dict(obj)

        if self._orient == 'records':
            obj.write_ndjson(file)
        else:
            obj.write_json(file)


class PandasJsonFileProcessor(JsonFileProcessor):
    def load(self, file):
        try:
            return pd.read_json(file, orient=self._orient, lines=True if self._orient == 'records' else False)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def dump(self, obj, file):
        assert isinstance(obj, pd.DataFrame) or isinstance(obj, pd.Series) or isinstance(obj, dict), (
            f'requires pd.DataFrame or pd.Series or dict, but {type(obj)} is passed.'
        )
        if isinstance(obj, dict):
            obj = pd.DataFrame.from_dict(obj)
        obj.to_json(file, orient=self._orient, lines=True if self._orient == 'records' else False)


class XmlFileProcessor(FileProcessor):
    def format(self):
        return None

    def load(self, file):
        try:
            return ET.parse(file)
        except ET.ParseError:
            return ET.ElementTree()

    def dump(self, obj, file):
        assert isinstance(obj, ET.ElementTree), f'requires ET.ElementTree, but {type(obj)} is passed.'
        obj.write(file)


class NpzFileProcessor(FileProcessor):
    def format(self):
        return luigi.format.Nop

    def load(self, file):
        return np.load(file)['data']

    def dump(self, obj, file):
        assert isinstance(obj, np.ndarray), f'requires np.ndarray, but {type(obj)} is passed.'
        np.savez_compressed(file, data=obj)


class ParquetFileProcessor(FileProcessor):
    def __init__(self, engine='pyarrow', compression=None):
        self._engine = engine
        self._compression = compression
        super().__init__()

    def format(self):
        return luigi.format.Nop

    def load(self, file): ...

    def dump(self, obj, file): ...


class PolarsParquetFileProcessor(ParquetFileProcessor):
    def load(self, file):
        if ObjectStorage.is_buffered_reader(file):
            return pl.read_parquet(file.name)
        else:
            return pl.read_parquet(BytesIO(file.read()))

    def dump(self, obj, file):
        assert isinstance(obj, (pl.DataFrame)), f'requires pl.DataFrame, but {type(obj)} is passed.'
        use_pyarrow = self._engine == 'pyarrow'
        compression = 'uncompressed' if self._compression is None else self._compression
        obj.write_parquet(file, use_pyarrow=use_pyarrow, compression=compression)


class PandasParquetFileProcessor(ParquetFileProcessor):
    def load(self, file):
        if ObjectStorage.is_buffered_reader(file):
            return pd.read_parquet(file.name)
        else:
            return pd.read_parquet(BytesIO(file.read()))

    def dump(self, obj, file):
        assert isinstance(obj, (pd.DataFrame)), f'requires pd.DataFrame, but {type(obj)} is passed.'
        # MEMO: to_parquet only supports a filepath as string (not a file handle)
        obj.to_parquet(file.name, index=False, engine=self._engine, compression=self._compression)


class FeatherFileProcessor(FileProcessor):
    def __init__(self, store_index_in_feather: bool):
        super().__init__()
        self._store_index_in_feather = store_index_in_feather
        self.INDEX_COLUMN_PREFIX = '__feather_gokart_index__'

    def format(self):
        return luigi.format.Nop

    def load(self, file): ...

    def dump(self, obj, file): ...


class PolarsFeatherFileProcessor(FeatherFileProcessor):
    def load(self, file):
        # Since polars' DataFrame doesn't have index, just load feather file
        # TODO: Fix ingnoring store_index_in_feather variable
        # Currently in PolarsFeatherFileProcessor, we ignored store_index_in_feather variable to avoid
        # a breaking change of FeatherFileProcessor's default behavior.
        if ObjectStorage.is_buffered_reader(file):
            return pl.read_ipc(file.name)
        return pl.read_ipc(BytesIO(file.read()))

    def dump(self, obj, file):
        assert isinstance(obj, (pl.DataFrame)), f'requires pl.DataFrame, but {type(obj)} is passed.'
        obj.write_ipc(file.name)


class PandasFeatherFileProcessor(FeatherFileProcessor):
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
                loaded_df = loaded_df.drop(columns={index_column})

        return loaded_df

    def dump(self, obj, file):
        assert isinstance(obj, (pd.DataFrame)), f'requires pd.DataFrame, but {type(obj)} is passed.'
        dump_obj = obj.copy()

        if self._store_index_in_feather:
            index_column_name = f'{self.INDEX_COLUMN_PREFIX}{dump_obj.index.name}'
            assert index_column_name not in dump_obj.columns, (
                f'column name {index_column_name} already exists in dump_obj. \
                Consider not saving index by setting store_index_in_feather=False.'
            )
            assert dump_obj.index.name != 'None', 'index name is "None", which is not allowed in gokart. Consider setting another index name.'

            dump_obj[index_column_name] = dump_obj.index
            dump_obj = dump_obj.reset_index(drop=True)

        # to_feather supports "binary" file-like object, but file variable is text
        dump_obj.to_feather(file.name)


if DATAFRAME_FRAMEWORK == 'polars':
    CsvFileProcessor = PolarsCsvFileProcessor  # type: ignore
    JsonFileProcessor = PolarsJsonFileProcessor  # type: ignore
    ParquetFileProcessor = PolarsParquetFileProcessor  # type: ignore
    FeatherFileProcessor = PolarsFeatherFileProcessor  # type: ignore
else:
    CsvFileProcessor = PandasCsvFileProcessor  # type: ignore
    JsonFileProcessor = PandasJsonFileProcessor  # type: ignore
    ParquetFileProcessor = PandasParquetFileProcessor  # type: ignore
    FeatherFileProcessor = PandasFeatherFileProcessor  # type: ignore


def make_file_processor(file_path: str, store_index_in_feather: bool) -> FileProcessor:
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
