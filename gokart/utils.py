from __future__ import annotations

import os
import sys
from typing import Iterable, TypeVar

import luigi


def add_config(file_path: str):
    _, ext = os.path.splitext(file_path)
    luigi.configuration.core.parser = ext
    assert luigi.configuration.add_config_path(file_path)


T = TypeVar('T')
if sys.version_info >= (3, 10):
    from typing import TypeAlias

    FlattenableItems: TypeAlias = T | Iterable['FlattenableItems[T]'] | dict[str, 'FlattenableItems[T]']
else:
    from typing import Union

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
