from __future__ import annotations

import bz2
import datetime
import json
import re
from collections.abc import Callable, Iterable
from logging import getLogger
from typing import Any, Generic, Protocol, Self, TypedDict, TypeVar, Unpack, cast
from warnings import warn

import luigi
from luigi import task_register
from luigi.parameter import ConfigPath, ParameterVisibility, _no_value, _NoValueType  # pyright: ignore[reportPrivateUsage]

import gokart


class ParameterKwargs(TypedDict, total=False):
    significant: bool
    description: str | None
    config_path: ConfigPath | None
    positional: bool
    always_in_help: bool
    batch_method: Callable[[Iterable[Any]], Any] | None
    visibility: ParameterVisibility


logger = getLogger(__name__)


TASK_ON_KART_TYPE = TypeVar('TASK_ON_KART_TYPE', bound='gokart.TaskOnKart[Any]')


class TaskInstanceParameter(luigi.Parameter[TASK_ON_KART_TYPE], Generic[TASK_ON_KART_TYPE]):
    def __init__(
        self,
        expected_type: type[TASK_ON_KART_TYPE] | None = None,
        default: TASK_ON_KART_TYPE | _NoValueType = _no_value,
        **kwargs: Unpack[ParameterKwargs],
    ):
        if expected_type is None:
            self.expected_type: type = gokart.TaskOnKart
        elif isinstance(expected_type, type):  # pyright: ignore[reportUnnecessaryIsInstance]  # runtime guard against non-type input despite the annotation
            self.expected_type = expected_type
        else:
            raise TypeError(f'expected_type must be a type, not {type(expected_type)}')
        super().__init__(default=default, **kwargs)

    @staticmethod
    def _recursive(param_dict: dict[str, Any]) -> Any:
        params = param_dict['params']
        task_cls = cast(type[gokart.TaskOnKart[Any]], cast(Any, task_register.Register).get_task_cls(param_dict['type']))
        for key, value in cast(list[tuple[str, luigi.Parameter[Any]]], task_cls.get_params()):
            if key in params:
                params[key] = cast(Any, value).parse(params[key])
        return task_cls(**params)

    @staticmethod
    def _recursive_decompress(s: str) -> dict[str, Any]:
        decompressed: dict[str, Any] = dict(cast(dict[str, Any], cast(Any, luigi.DictParameter()).parse(s)))
        if 'params' in decompressed:
            decompressed['params'] = TaskInstanceParameter._recursive_decompress(bz2.decompress(bytes.fromhex(decompressed['params'])).decode())
        return decompressed

    def parse(self, x: str | dict[str, Any]) -> TASK_ON_KART_TYPE:
        if isinstance(x, str):
            x = self._recursive_decompress(x)
        return self._recursive(x)

    def serialize(self, x: TASK_ON_KART_TYPE) -> str:
        params = bz2.compress(json.dumps(x.to_str_params(only_significant=True)).encode()).hex()
        values = {'type': x.get_task_family(), 'params': params}
        return cast(str, cast(Any, luigi.DictParameter()).serialize(values))

    def _warn_on_wrong_param_type(self, param_name: str, param_value: Any) -> None:
        if not isinstance(param_value, self.expected_type):
            raise TypeError(f'{param_value} is not an instance of {self.expected_type}')


class _TaskInstanceEncoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, luigi.Task):
            return cast(str, cast(Any, TaskInstanceParameter()).serialize(o))
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, o)


class ListTaskInstanceParameter(luigi.Parameter[list[TASK_ON_KART_TYPE]], Generic[TASK_ON_KART_TYPE]):
    def __init__(
        self,
        expected_elements_type: type[TASK_ON_KART_TYPE] | None = None,
        default: list[TASK_ON_KART_TYPE] | _NoValueType = _no_value,
        **kwargs: Unpack[ParameterKwargs],
    ):
        if expected_elements_type is None:
            self.expected_elements_type: type = gokart.TaskOnKart
        elif isinstance(expected_elements_type, type):  # pyright: ignore[reportUnnecessaryIsInstance]
            self.expected_elements_type = expected_elements_type
        else:
            raise TypeError(f'expected_elements_type must be a type, not {type(expected_elements_type)}')
        super().__init__(default=default, **kwargs)

    def parse(self, x: str) -> list[TASK_ON_KART_TYPE]:
        return [TaskInstanceParameter().parse(item) for item in list(json.loads(x))]

    def serialize(self, x: list[TASK_ON_KART_TYPE]) -> str:
        return json.dumps(x, cls=_TaskInstanceEncoder)

    def _warn_on_wrong_param_type(self, param_name: str, param_value: Any) -> None:
        for v in param_value:
            if not isinstance(v, self.expected_elements_type):
                raise TypeError(f'{v} is not an instance of {self.expected_elements_type}')


class ExplicitBoolParameter(luigi.BoolParameter):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(luigi.BoolParameter, self).__init__(*args, **kwargs)

    def _parser_kwargs(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return cast(dict[str, Any], cast(Any, luigi.Parameter)._parser_kwargs(*args, **kwargs))


class Serializable(Protocol):
    def gokart_serialize(self) -> str:
        """Implement this method to serialize the object as an parameter
        You can omit some fields from results of serialization if you want to ignore changes of them
        """
        ...

    @classmethod
    def gokart_deserialize(cls, s: str) -> Self:
        """Implement this method to deserialize the object from a string"""
        ...


S = TypeVar('S', bound=Serializable)


class SerializableParameter(luigi.Parameter[S], Generic[S]):
    def __init__(self, object_type: type[S], *args: Any, **kwargs: Any) -> None:
        self._object_type = object_type
        super().__init__(*args, **kwargs)

    def parse(self, x: str) -> S:
        return self._object_type.gokart_deserialize(x)

    def serialize(self, x: S) -> str:
        return x.gokart_serialize()


class ZonedDateSecondParameter(luigi.Parameter[datetime.datetime]):
    """
    ZonedDateSecondParameter supports a datetime.datetime object with timezone information.

    A ZonedDateSecondParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date, time specified to the second and timezone. For example, ``2013-07-10T19:07:38+09:00`` specifies July 10, 2013 at
    19:07:38 +09:00. The separator `:` can be omitted for Python3.11 and later.
    """

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    def parse(self, x: str) -> datetime.datetime:
        # special character 'Z' is replaced with '+00:00'
        # because Python 3.11 and later support fromisoformat with Z at the end of the string.
        if x.endswith('Z'):
            x = x[:-1] + '+00:00'
        dt = datetime.datetime.fromisoformat(x)
        if dt.tzinfo is None:
            warn('The input does not have timezone information. Please consider using luigi.DateSecondParameter instead.', stacklevel=1)
        return dt

    def serialize(self, x: datetime.datetime) -> str:
        return x.isoformat()

    def normalize(self, x: datetime.datetime) -> datetime.datetime:
        # override _DatetimeParameterBase.normalize to avoid do nothing to normalize except removing microsecond.
        # microsecond is removed because the number of digits of microsecond is not fixed.
        # See also luigi's implementation  https://github.com/spotify/luigi/blob/v3.6.0/luigi/parameter.py#L612
        return x.replace(microsecond=0)


_BYTE_SIZE_UNITS: dict[str, int] = {
    'kb': 1000,
    'mb': 1000**2,
    'gb': 1000**3,
    'kib': 1024,
    'mib': 1024**2,
    'gib': 1024**3,
}
_BYTE_SIZE_PATTERN = re.compile(r'(?P<number>\d+)[ \t]*(?P<unit>[kmg]i?b)?', re.IGNORECASE | re.ASCII)


class ByteSizeParameter(luigi.Parameter[int]):
    """
    Parameter whose value is a number of bytes, written as an ``int`` or with a unit.

    Decimal units (``10KB``, ``10MB``, ``10GB``) are powers of 1000 and binary units (``10KiB``, ``10MiB``, ``10GiB``) are powers of 1024.
    Units are case-insensitive and may be separated from the number by spaces. The number must be a non-negative integer.
    """

    def parse(self, x: str) -> int:
        # a TOML luigi config hands over a bare number as an int, as luigi.IntParameter accepts
        if isinstance(x, int) and not isinstance(x, bool):  # pyright: ignore[reportUnnecessaryIsInstance]
            return x
        match = _BYTE_SIZE_PATTERN.fullmatch(x)
        if match is None:
            # a bare M / K / G is rejected since it is unclear whether it means MB or MiB
            raise luigi.parameter.ParameterException(
                f'Invalid byte size {x!r}: use a non-negative integer of bytes, or one with KB / MB / GB (1000) or KiB / MiB / GiB (1024).'
            )
        unit = match['unit']
        return int(match['number']) * (_BYTE_SIZE_UNITS[unit.lower()] if unit else 1)

    def serialize(self, x: int) -> str:
        return str(x)


class OptionalByteSizeParameter(luigi.parameter.OptionalParameterMixin[int], ByteSizeParameter):  # type: ignore[misc]
    """ByteSizeParameter that also takes ``None``, which an empty string parses to."""

    expected_type = int
