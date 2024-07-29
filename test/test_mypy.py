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
    baz: bool = gokart.ExplicitBoolParameter()

MyTask(foo=1, bar='bar', baz=False)
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
MyTask(foo='1', baz='not bool')
        """

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('error: Argument "baz" to "MyTask" has incompatible type "str"; expected "bool"  [arg-type]', result[0])
            self.assertIn('Found 2 errors in 1 file (checked 1 source file)', result[0])
