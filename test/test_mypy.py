import tempfile
import unittest

from mypy import api

from test.config import PYPROJECT_TOML


class TestMyMypyPlugin(unittest.TestCase):
    def test_plugin_no_issue(self):
        test_code = """
import luigi
from luigi import Parameter
import gokart
import datetime


class MyTask(gokart.TaskOnKart):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    foo: int = luigi.IntParameter() # type: ignore
    bar: str = luigi.Parameter() # type: ignore
    baz: bool = gokart.ExplicitBoolParameter()
    qux: str = Parameter()
    # https://github.com/m3dev/gokart/issues/395
    datetime: datetime.datetime = luigi.DateMinuteParameter(interval=10, default=datetime.datetime(2021, 1, 1))



# TaskOnKart parameters:
#   - `complete_check_at_run`
MyTask(foo=1, bar='bar', baz=False, qux='qux', complete_check_at_run=False)
"""

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('Success: no issues found', result[0])

    def test_plugin_invalid_arg(self):
        test_code = """
import luigi
import gokart


class MyTask(gokart.TaskOnKart):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    foo: int = luigi.IntParameter() # type: ignore
    bar: str = luigi.Parameter() # type: ignore
    baz: bool = gokart.ExplicitBoolParameter()

# issue: foo is int
# not issue: bar is missing, because it can be set by config file.
# TaskOnKart parameters:
#   - `complete_check_at_run`
MyTask(foo='1', baz='not bool', complete_check_at_run='not bool')
        """

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('error: Argument "baz" to "MyTask" has incompatible type "str"; expected "bool"  [arg-type]', result[0])
            self.assertIn('error: Argument "complete_check_at_run" to "MyTask" has incompatible type "str"; expected "bool"  [arg-type]', result[0])
            self.assertIn('Found 3 errors in 1 file (checked 1 source file)', result[0])

    def test_parameter_has_default_type_invalid_pattern(self):
        """
        If user doesn't set the type of the parameter, mypy infer the default type from Parameter types.
        """
        test_code = """
import enum
import luigi
import gokart


class MyEnum(enum.Enum):
    FOO = enum.auto()

class MyTask(gokart.TaskOnKart):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    foo = luigi.IntParameter()
    bar = luigi.DateParameter()
    baz = gokart.TaskInstanceParameter()
    qux = luigi.NumericalParameter(var_type=int)
    quux = luigi.ChoiceParameter(choices=[1, 2, 3], var_type=int)
    corge = luigi.EnumParameter(enum=MyEnum)

MyTask(foo="1", bar=1, baz=1, qux='1', quux='1', corge=1)
"""
        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--show-traceback', '--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('error: Argument "bar" to "MyTask" has incompatible type "int"; expected "date"  [arg-type]', result[0])
            self.assertIn('error: Argument "baz" to "MyTask" has incompatible type "int"; expected "TaskOnKart[Any]"  [arg-type]', result[0])
            self.assertIn('error: Argument "qux" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('error: Argument "quux" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('error: Argument "corge" to "MyTask" has incompatible type "int"; expected "MyEnum"  [arg-type]', result[0])

    def test_parameter_has_default_type_no_issue_pattern(self):
        """
        If user doesn't set the type of the parameter, mypy infer the default type from Parameter types.
        """
        test_code = """
from datetime import date
import luigi
import gokart

class MyTask(gokart.TaskOnKart):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    foo = luigi.IntParameter()
    bar = luigi.DateParameter()
    baz = gokart.TaskInstanceParameter()

MyTask(foo=1, bar=date.today(), baz=gokart.TaskOnKart())
"""
        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--show-traceback', '--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('Success: no issues found', result[0])
