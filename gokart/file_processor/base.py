from __future__ import annotations

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
        dumped = self._fast_dumps(obj)
        file.write(dumped if dumped is not None else cast(Any, dill).dumps(obj, protocol=4))

    def _fast_dumps(self, obj: Any) -> bytes | None:
        buffer = BytesIO()
        try:
            self._Pickler(buffer, protocol=4).dump(obj)
        except Exception as e:
            logger.debug(f'falling back to dill: {e!r}')
            return None
        return buffer.getvalue()


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
