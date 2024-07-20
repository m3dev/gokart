import tempfile
import unittest

from mypy import api

from test.config import PYPROJECT_TOML


class TestMyMypyPlugin(unittest.TestCase):
    def test_plugin_no_issue(self):
        test_code = """
import luigi
import gokart


class MyTask(gokart.TaskOnKart):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    foo: int = luigi.IntParameter() # type: ignore
    bar: str = luigi.Parameter() # type: ignore

MyTask(foo=1, bar='bar')
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

MyTask(foo='1') # foo is int, but it is str and bar is missing.
"""

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('error: Missing named argument "bar" for "MyTask"  [call-arg]', result[0])
            self.assertIn('error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('Found 2 errors in 1 file (checked 1 source file)', result[0])

    def test_plugin_instance_parameter_no_issuee(self):
        test_code = """
import gokart


class StrFoo(gokart.TaskOnKart[str]):
    def run(self):
        self.dump('foo')


class Bar(gokart.TaskOnKart[str]):
    str_foo: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()

    def run(self):
        self.dump('bar')


bar = Bar(str_foo=StrFoo())
"""

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('Success: no issues found', result[0])

    def test_plugin_instance_parameter_invalid_arg(self):
        test_code = """
import gokart

class IntFoo(gokart.TaskOnKart[int]):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    a: str = luigi.Parameter() # type: ignore
    def run(self):
        self.dump(1)


class Bar(gokart.TaskOnKart[int]):
    str_foo: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()

    def run(self):
        self.dump(2)


bar = Bar(str_foo=IntFoo(a="a"))
"""

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--tb', '--config-file', str(PYPROJECT_TOML), test_file.name])
            print(result)
            self.assertIn('error: Argument "str_foo" to "Bar" has incompatible type "IntFoo"; expected "TaskOnKart[str]"  [arg-type]', result[0])
            self.assertIn('Found 1 error in 1 file (checked 1 source file)', result[0])
