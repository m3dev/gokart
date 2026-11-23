from __future__ import annotations

import io
import pickle
import types
import xml.etree.ElementTree as ET
from abc import abstractmethod
from io import BytesIO
from logging import getLogger
from typing import Any, Literal, cast

import dill  # pyright: ignore[reportMissingTypeStubs]
import luigi
import luigi.format
import numpy as np

from gokart.utils import load_dill_with_pandas_backward_compatibility

logger = getLogger(__name__)

# Type alias for DataFrame library return type
DataFrameType = Literal['pandas', 'polars', 'polars-lazy']


class FileProcessor:
    @abstractmethod
    def format(self) -> Any: ...

    @abstractmethod
    def load(self, file: Any) -> Any: ...

    @abstractmethod
    def dump(self, obj: Any, file: Any) -> None: ...


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

    def load(self, file: Any):
        return file.read()

    def dump(self, obj: Any, file: Any):
        file.write(obj)


class PickleFileProcessor(FileProcessor):
    class _Pickler(pickle.Pickler):
        """The C pickler, which dumps much faster than dill's pure-Python pickler.

        It refuses the objects whose dill output loads differently, so that dump falls back to dill:
        anything defined in __main__ (dill stores it by value, pickle by name only),
        bound methods (pickle restores them by looking up the function's __name__ on the instance),
        and ndarray subclasses with instance attributes (pickle drops the attributes).
        """

        def reducer_override(self, obj: Any) -> Any:
            if (
                getattr(obj, '__module__', None) == '__main__'
                or isinstance(obj, types.MethodType)
                or (isinstance(obj, np.ndarray) and getattr(cast(Any, obj), '__dict__', None))
            ):
                raise pickle.PicklingError(f'dill pickles {type(cast(Any, obj))!r} differently')
            if isinstance(obj, np.ndarray) and not obj.flags.writeable:
                # Protocol 5 stores a read-only buffer as bytes, which loads back read-only (pandas then refuses in-place writes).
                # The protocol 4 reduction loads a writable copy, as before the switch to protocol 5.
                return obj.__reduce_ex__(4)
            return NotImplemented

    def format(self):
        return luigi.format.Nop

    def load(self, file: Any):
        if not file.seekable():
            # load_dill_with_pandas_backward_compatibility() requires file with seek() and readlines() implemented.
            # Therefore, we need to wrap with BytesIO which makes file seekable and readlinesable.
            # For example, ReadableS3File is not a seekable file.
            return load_dill_with_pandas_backward_compatibility(BytesIO(file.read()))
        return load_dill_with_pandas_backward_compatibility(file)

    def dump(self, obj: Any, file: Any):
        if not self._can_roll_back(file):
            # A failed C pickler could not be rolled back, so build the pickle in memory first.
            buffer = BytesIO()
            self._dump_to(obj, buffer)
            file.write(buffer.getvalue())
            return
        # Writing straight to the file keeps the peak memory at about the size of obj, instead of obj plus the whole pickle.
        self._dump_to(obj, file)

    @staticmethod
    def _can_roll_back(file: Any) -> bool:
        # This has to be known before anything is written. seekable() alone is not enough: GzipFile is "seekable"
        # but cannot seek back over what it has already compressed, and a buffered writer seeks through the stream it wraps.
        # luigi's atomic files (local, GCS, S3) are BufferedWriters over a FileIO. A FileIO on a pipe is not seekable.
        raw: object = cast(Any, file).raw if isinstance(file, io.BufferedWriter | io.BufferedRandom) else file
        return isinstance(raw, io.FileIO | io.BytesIO) and file.seekable()

    def _dump_to(self, obj: Any, file: Any) -> None:
        start = file.tell()
        try:
            # protocol 5 writes the buffers of numpy arrays and Arrow-backed columns without copying them first.
            self._Pickler(file, protocol=5).dump(obj)
        except OSError:
            # A write error such as a full disk would hit dill too, only after redoing the whole dump.
            raise
        except Exception as e:
            logger.debug(f'falling back to dill: {e!r}')
            # drop what the C pickler wrote before it failed
            file.seek(start)
            file.truncate()
            cast(Any, dill).dump(obj, file, protocol=5)


class TextFileProcessor(FileProcessor):
    def format(self):
        return None

    def load(self, file: Any):
        return [s.rstrip() for s in file.readlines()]

    def dump(self, obj: Any, file: Any):
        if isinstance(obj, list):
            for x in cast(list[Any], obj):
                file.write(str(x) + '\n')
        else:
            file.write(str(obj))


class GzipFileProcessor(FileProcessor):
    def format(self):
        return luigi.format.Gzip

    def load(self, file: Any):
        return [s.rstrip().decode() for s in file.readlines()]

    def dump(self, obj: Any, file: Any):
        if isinstance(obj, list):
            for x in cast(list[Any], obj):
                file.write((str(x) + '\n').encode())
        else:
            file.write(str(obj).encode())


class XmlFileProcessor(FileProcessor):
    def format(self):
        return None

    def load(self, file: Any):
        try:
            return ET.parse(file)
        except ET.ParseError:
            return ET.ElementTree()

    def dump(self, obj: Any, file: Any):
        assert isinstance(obj, ET.ElementTree), f'requires ET.ElementTree, but {type(obj)} is passed.'
        obj.write(file)


class NpzFileProcessor(FileProcessor):
    def format(self):
        return luigi.format.Nop

    def load(self, file: Any):
        return np.load(file)['data']

    def dump(self, obj: Any, file: Any):
        assert isinstance(obj, np.ndarray), f'requires np.ndarray, but {type(obj)} is passed.'
        cast(Any, np).savez_compressed(file, data=obj)
