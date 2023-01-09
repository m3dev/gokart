import os
import pickle
import xml.etree.ElementTree as ET
from abc import abstractmethod
from logging import getLogger

import luigi
import luigi.contrib.s3
import luigi.format
import numpy as np
import pandas as pd
import pandas.errors

from gokart.object_storage import ObjectStorage

logger = getLogger(__name__)


class FileProcessor(object):

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


class _ChunkedLargeFileReader(object):

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
                buffer[idx:idx + batch_size] = self._file.read(batch_size)
                idx += batch_size
            logger.info('done.')
            return buffer
        return self._file.read(n)


class PickleFileProcessor(FileProcessor):

    def format(self):
        return luigi.format.Nop

    def load(self, file):
        if not ObjectStorage.is_buffered_reader(file):
            return pickle.loads(file.read())
        return pickle.load(_ChunkedLargeFileReader(file))

    def dump(self, obj, file):
        self._write(pickle.dumps(obj, protocol=4), file)

    @staticmethod
    def _write(buffer, file):
        n = len(buffer)
        idx = 0
        while idx < n:
            logger.info(f'writing a file with total_bytes={n}...')
            batch_size = min(n - idx, 1 << 31 - 1)
            logger.info(f'writing bytes [{idx}, {idx + batch_size})')
            file.write(buffer[idx:idx + batch_size])
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

    def __init__(self, sep=','):
        self._sep = sep
        super(CsvFileProcessor, self).__init__()

    def format(self):
        return None

    def load(self, file):
        try:
            return pd.read_csv(file, sep=self._sep)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def dump(self, obj, file):
        assert isinstance(obj, (pd.DataFrame, pd.Series)), \
            f'requires pd.DataFrame or pd.Series, but {type(obj)} is passed.'
        obj.to_csv(file, mode='wt', index=False, sep=self._sep, header=True)


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

    def format(self):
        return None

    def load(self, file):
        try:
            return pd.read_json(file)
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    def dump(self, obj, file):
        assert isinstance(obj, pd.DataFrame) or isinstance(obj, pd.Series) or isinstance(obj, dict), \
            f'requires pd.DataFrame or pd.Series or dict, but {type(obj)} is passed.'
        if isinstance(obj, dict):
            obj = pd.DataFrame.from_dict(obj)
        obj.to_json(file)


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
        super(ParquetFileProcessor, self).__init__()

    def format(self):
        return None

    def load(self, file):
        # MEMO: read_parquet only supports a filepath as string (not a file handle)
        return pd.read_parquet(file.name)

    def dump(self, obj, file):
        assert isinstance(obj, (pd.DataFrame)), \
            f'requires pd.DataFrame, but {type(obj)} is passed.'
        # MEMO: to_parquet only supports a filepath as string (not a file handle)
        obj.to_parquet(file.name, index=False, compression=self._compression)


class FeatherFileProcessor(FileProcessor):

    def __init__(self, store_index_in_feather: bool):
        super(FeatherFileProcessor, self).__init__()
        self._store_index_in_feather = store_index_in_feather
        self.INDEX_COLUMN_PREFIX = '__feather_gokart_index__'

    def format(self):
        return None

    def load(self, file):
        loaded_df = pd.read_feather(file.name)

        if self._store_index_in_feather:
            if any(col.startswith(self.INDEX_COLUMN_PREFIX) for col in loaded_df.columns):
                index_columns = [col_name for col_name in loaded_df.columns[::-1] if col_name[:len(self.INDEX_COLUMN_PREFIX)] == self.INDEX_COLUMN_PREFIX]
                index_column = index_columns[0]
                index_name = index_column[len(self.INDEX_COLUMN_PREFIX):]
                loaded_df.index = pd.Index(loaded_df[index_column], name=index_name)
                loaded_df = loaded_df.drop(columns={index_column})

        return loaded_df

    def dump(self, obj, file):
        assert isinstance(obj, (pd.DataFrame)), \
            f'requires pd.DataFrame, but {type(obj)} is passed.'
        dump_obj = obj.copy()

        if self._store_index_in_feather:
            index_column_name = f'{self.INDEX_COLUMN_PREFIX}{dump_obj.index.name}'
            assert index_column_name not in dump_obj.columns, f'column name {index_column_name} already exists in dump_obj. \
                Consider not saving index by setting store_index_in_feather=False.'

            dump_obj[index_column_name] = dump_obj.index
            dump_obj = dump_obj.reset_index(drop=True)

        # to_feather supports "binary" file-like object, but file variable is text
        dump_obj.to_feather(file.name)


def make_file_processor(file_path: str, store_index_in_feather: bool) -> FileProcessor:
    extension2processor = {
        '.txt': TextFileProcessor(),
        '.csv': CsvFileProcessor(sep=','),
        '.tsv': CsvFileProcessor(sep='\t'),
        '.pkl': PickleFileProcessor(),
        '.gz': GzipFileProcessor(),
        '.json': JsonFileProcessor(),
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
