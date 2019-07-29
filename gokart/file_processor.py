import pickle
import os
from abc import abstractmethod
from logging import getLogger
import xml.etree.ElementTree as ET

import luigi
import luigi.contrib.s3
import luigi.format
import pandas as pd
import pandas.errors

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


class _LargeLocalFileReader(object):
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
        if isinstance(file, luigi.contrib.s3.ReadableS3File):
            return pickle.loads(file.read())
        return pickle.load(_LargeLocalFileReader(file))

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
        obj.to_csv(file, index=False, sep=self._sep, header=True)


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


def make_file_processor(file_path: str) -> FileProcessor:
    extension2processor = {
        '.txt': TextFileProcessor(),
        '.csv': CsvFileProcessor(sep=','),
        '.tsv': CsvFileProcessor(sep='\t'),
        '.pkl': PickleFileProcessor(),
        '.gz': GzipFileProcessor(),
        '.json': JsonFileProcessor(),
        '.xml': XmlFileProcessor()
    }

    extension = os.path.splitext(file_path)[1]
    assert extension in extension2processor, f'{extension} is not supported. The supported extensions are {list(extension2processor.keys())}.'
    return extension2processor[extension]
