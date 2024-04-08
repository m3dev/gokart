from __future__ import annotations

import os
from typing import Iterable, TypeAlias, TypeVar

import luigi


def add_config(file_path: str):
    _, ext = os.path.splitext(file_path)
    luigi.configuration.core.parser = ext
    assert luigi.configuration.add_config_path(file_path)


T = TypeVar('T')
FlattableItems: TypeAlias = T | Iterable['FlattableItems[T]'] | dict[str, 'FlattableItems[T]']


def flatten(targets: FlattableItems[T]) -> list[T]:
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
