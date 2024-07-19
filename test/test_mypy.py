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
    # TODO: remove the following comments after fixing the issue.
    foo: int = luigi.IntParameter() # type: ignore
    bar: str = luigi.Parameter() # type: ignore

MyTask(foo=1, bar='bar')
        """

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('Success: no issues found', result[0])

    def test_plugin_invalid_arg(self):
        test_code = """
import luigi
import gokart


class MyTask(gokart.TaskOnKart):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    # TODO: remove the following comments after fixing the issue.
    foo: int = luigi.IntParameter() # type: ignore
    bar: str = luigi.Parameter() # type: ignore

MyTask(foo='1') # foo is int, but it is str and bar is missing.
        """

        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            result = api.run(['--config-file', str(PYPROJECT_TOML), test_file.name])
            self.assertIn('error: Missing named argument "bar" for "MyTask"  [call-arg]', result[0])
            self.assertIn('error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', result[0])
            self.assertIn('Found 2 errors in 1 file (checked 1 source file)', result[0])
