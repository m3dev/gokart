from __future__ import annotations

import xml.etree.ElementTree as ET
from abc import abstractmethod
from io import BytesIO
from logging import getLogger
from typing import Literal

import dill
import luigi
import luigi.format
import numpy as np

from gokart.utils import load_dill_with_pandas_backward_compatibility

logger = getLogger(__name__)

# Type alias for DataFrame library return type
DataFrameType = Literal['pandas', 'polars', 'polars-lazy']


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
