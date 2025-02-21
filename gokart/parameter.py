import bz2
import datetime
import json
from logging import getLogger
from typing import Generic, Protocol, TypeVar

import luigi
from luigi import task_register
from luigi.parameter import _DatetimeParameterBase

import gokart

logger = getLogger(__name__)


class TaskInstanceParameter(luigi.Parameter):
    def __init__(self, expected_type=None, *args, **kwargs):
        if expected_type is None:
            self.expected_type: type = gokart.TaskOnKart
        elif isinstance(expected_type, type):
            self.expected_type = expected_type
        else:
            raise TypeError(f'expected_type must be a type, not {type(expected_type)}')
        super().__init__(*args, **kwargs)

    @staticmethod
    def _recursive(param_dict):
        params = param_dict['params']
        task_cls = task_register.Register.get_task_cls(param_dict['type'])
        for key, value in task_cls.get_params():
            if key in params:
                params[key] = value.parse(params[key])
        return task_cls(**params)

    @staticmethod
    def _recursive_decompress(s):
        s = dict(luigi.DictParameter().parse(s))
        if 'params' in s:
            s['params'] = TaskInstanceParameter._recursive_decompress(bz2.decompress(bytes.fromhex(s['params'])).decode())
        return s

    def parse(self, s):
        if isinstance(s, str):
            s = self._recursive_decompress(s)
        return self._recursive(s)

    def serialize(self, x):
        params = bz2.compress(json.dumps(x.to_str_params(only_significant=True)).encode()).hex()
        values = dict(type=x.get_task_family(), params=params)
        return luigi.DictParameter().serialize(values)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        if not isinstance(param_value, self.expected_type):
            raise TypeError(f'{param_value} is not an instance of {self.expected_type}')


class _TaskInstanceEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, luigi.Task):
            return TaskInstanceParameter().serialize(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class ListTaskInstanceParameter(luigi.Parameter):
    def __init__(self, expected_elements_type=None, *args, **kwargs):
        if expected_elements_type is None:
            self.expected_elements_type: type = gokart.TaskOnKart
        elif isinstance(expected_elements_type, type):
            self.expected_elements_type = expected_elements_type
        else:
            raise TypeError(f'expected_elements_type must be a type, not {type(expected_elements_type)}')
        super().__init__(*args, **kwargs)

    def parse(self, s):
        return [TaskInstanceParameter().parse(x) for x in list(json.loads(s))]

    def serialize(self, x):
        return json.dumps(x, cls=_TaskInstanceEncoder)

    def _warn_on_wrong_param_type(self, param_name, param_value):
        for v in param_value:
            if not isinstance(v, self.expected_elements_type):
                raise TypeError(f'{v} is not an instance of {self.expected_elements_type}')


class ExplicitBoolParameter(luigi.BoolParameter):
    def __init__(self, *args, **kwargs):
        luigi.Parameter.__init__(self, *args, **kwargs)

    def _parser_kwargs(self, *args, **kwargs):  # type: ignore
        return luigi.Parameter._parser_kwargs(*args, *kwargs)


T = TypeVar('T')


class Serializable(Protocol):
    def gokart_serialize(self) -> str:
        """Implement this method to serialize the object as an parameter
        You can omit some fields from results of serialization if you want to ignore changes of them
        """
        ...

    @classmethod
    def gokart_deserialize(cls: type[T], s: str) -> T:
        """Implement this method to deserialize the object from a string"""
        ...


S = TypeVar('S', bound=Serializable)


class SerializableParameter(luigi.Parameter, Generic[S]):
    def __init__(self, object_type: type[S], *args, **kwargs):
        self._object_type = object_type
        super().__init__(*args, **kwargs)

    def parse(self, s: str) -> S:
        return self._object_type.gokart_deserialize(s)

    def serialize(self, x: S) -> str:
        return x.gokart_serialize()


class TimestampParameter(_DatetimeParameterBase):
    """
    TimestampParameter supprts a datetime.datetime object with timezone information.

    A TimestampParameter is a `ISO 8601 <http://en.wikipedia.org/wiki/ISO_8601>`_ formatted
    date, time specified to the second and timezone. For example, ``2013-07-10T190738Z+0900`` specifies July 10, 2013 at
    19:07:38 +09:00.
    """

    date_format = '%Y-%m-%dT%H%M%SZ%z'
    _timedelta = datetime.timedelta(seconds=1)

    def __init__(self, interval=1, start=None, **kwargs):
        super().__init__(interval, start, **kwargs)

    def normalize(self, dt):
        # override _DatetimeParameterBase.normalize to avoid do nothing to normalize except removing microsecond.
        # microsecond is removed because the number of digits of microsecond is not fixed.
        # See also luigi's implementation  https://github.com/spotify/luigi/blob/v3.6.0/luigi/parameter.py#L612
        return dt.replace(microsecond=0)
