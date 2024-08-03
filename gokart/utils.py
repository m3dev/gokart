from __future__ import annotations

import os
import sys
from io import BytesIO
from typing import Any, Iterable, Protocol, TypeVar, Union

import dill
import luigi
import pandas as pd


class FileLike(Protocol):
    def read(self, n: int) -> bytes: ...

    def readline(self) -> bytes: ...

    def seek(self, offset: int) -> None: ...

    def seekable(self) -> bool: ...


def add_config(file_path: str):
    _, ext = os.path.splitext(file_path)
    luigi.configuration.core.parser = ext  # type: ignore
    assert luigi.configuration.add_config_path(file_path)


T = TypeVar('T')
if sys.version_info >= (3, 10):
    from typing import TypeAlias

    FlattenableItems: TypeAlias = T | Iterable['FlattenableItems[T]'] | dict[str, 'FlattenableItems[T]']
else:
    FlattenableItems = Union[T, Iterable['FlattenableItems[T]'], dict[str, 'FlattenableItems[T]']]


def flatten(targets: FlattenableItems[T]) -> list[T]:
    """
    Creates a flat list of all items in structured output (dicts, lists, items):

    .. code-block:: python

        >>> sorted(flatten({'a': 'foo', 'b': 'bar'}))
        ['bar', 'foo']
        >>> sorted(flatten(['foo', ['bar', 'troll']]))
        ['bar', 'foo', 'troll']
        >>> flatten('foo')
        ['foo']
        >>> flatten(42)
        [42]

    This method is copied and modified from [luigi.task.flatten](https://github.com/spotify/luigi/blob/367edc2e3a099b8a0c2d15b1676269e33ad06117/luigi/task.py#L958) in accordance with [Apache License 2.0](https://github.com/spotify/luigi/blob/367edc2e3a099b8a0c2d15b1676269e33ad06117/LICENSE).
    """
    if targets is None:
        return []
    flat = []
    if isinstance(targets, dict):
        for _, result in targets.items():
            flat += flatten(result)
        return flat

    if isinstance(targets, str):
        return [targets]  # type: ignore

    if not isinstance(targets, Iterable):
        return [targets]

    for result in targets:
        flat += flatten(result)
    return flat


def load_dill_with_pandas_backward_compatibility(file: Union[FileLike, BytesIO]) -> Any:
    """Load binary dumped by dill with pandas backward compatibility.
    pd.read_pickle can load binary dumped in backward pandas version, and also any objects dumped by pickle.
    It is unclear whether all objects dumped by dill can be loaded by pd.read_pickle, we use dill.load as a fallback.
    """
    try:
        return dill.load(file)
    except Exception:
        assert file.seekable(), f'{file} is not seekable.'
        file.seek(0)
        return pd.read_pickle(file)
