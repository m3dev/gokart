import json
import tempfile
from dataclasses import asdict, dataclass

import luigi
import pytest
from luigi.cmdline_parser import CmdlineParser
from mypy import api

from gokart import SerializableParameter, TaskOnKart
from test.config import PYPROJECT_TOML


@dataclass(frozen=True)
class Config:
    foo: int
    bar: str

    def gokart_serialize(self) -> str:
        # dict is ordered in Python 3.7+
        return json.dumps(asdict(self))

    @classmethod
    def gokart_deserialize(cls, s: str) -> 'Config':
        return cls(**json.loads(s))


class SerializableParameterWithOutDefault(TaskOnKart):
    task_namespace = __name__
    config: Config = SerializableParameter(object_type=Config)

    def run(self):
        self.dump(self.config)


class SerializableParameterWithDefault(TaskOnKart):
    task_namespace = __name__
    config: Config = SerializableParameter(object_type=Config, default=Config(foo=1, bar='bar'))

    def run(self):
        self.dump(self.config)


class TestSerializableParameter:
    def test_default(self):
        with CmdlineParser.global_instance([f'{__name__}.SerializableParameterWithDefault']) as cp:
            assert cp.get_task_obj().config == Config(foo=1, bar='bar')

    def test_parse_param(self):
        with CmdlineParser.global_instance([f'{__name__}.SerializableParameterWithOutDefault', '--config', '{"foo": 100, "bar": "val"}']) as cp:
            assert cp.get_task_obj().config == Config(foo=100, bar='val')

    def test_missing_parameter(self):
        with pytest.raises(luigi.parameter.MissingParameterException):
            with CmdlineParser.global_instance([f'{__name__}.SerializableParameterWithOutDefault']) as cp:
                cp.get_task_obj()

    def test_value_error(self):
        with pytest.raises(ValueError):
            with CmdlineParser.global_instance([f'{__name__}.SerializableParameterWithOutDefault', '--config', 'Foo']) as cp:
                cp.get_task_obj()

    def test_expected_one_argument_error(self):
        with pytest.raises(SystemExit):
            with CmdlineParser.global_instance([f'{__name__}.SerializableParameterWithOutDefault', '--config']) as cp:
                cp.get_task_obj()

    def test_mypy(self):
        """check invalid object cannot used for SerializableParameter"""

        test_code = """
import gokart

class InvalidClass:
    ...

gokart.SerializableParameter(object_type=InvalidClass)
        """
        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            assert 'Value of type variable "S" of "SerializableParameter" cannot be "InvalidClass"  [type-var]' in result[0]
