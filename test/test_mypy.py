import importlib
import tempfile
import unittest

from mypy import api

from test.config import PYPROJECT_TOML


class TestMypyWithoutPlugin(unittest.TestCase):
    """mypy checks parameters annotated with the parameter type through dataclass_transform, without any plugin."""

    def test_plugin_is_removed(self):
        with self.assertRaises(ModuleNotFoundError):
            importlib.import_module('gokart.mypy')

    def _run_mypy(self, test_code: str) -> tuple[str, str, int]:
        with tempfile.NamedTemporaryFile(suffix='.py') as test_file:
            test_file.write(test_code.encode('utf-8'))
            test_file.flush()
            return api.run(['--no-incremental', '--cache-dir=/dev/null', '--config-file', str(PYPROJECT_TOML), test_file.name])

    def test_no_issue(self):
        test_code = """
import luigi
import gokart


class MyTask(gokart.TaskOnKart[str]):
    foo: luigi.IntParameter = luigi.IntParameter()
    bar: luigi.Parameter = luigi.Parameter()
    baz: gokart.ExplicitBoolParameter = gokart.ExplicitBoolParameter()


MyTask(foo=1, bar='bar', baz=False, complete_check_at_run=False)
"""
        stdout, stderr, exitcode = self._run_mypy(test_code)
        self.assertEqual(exitcode, 0, f'mypy error occurred:\nstdout: {stdout}\nstderr: {stderr}')
        self.assertIn('Success: no issues found', stdout)

    def test_invalid_arg(self):
        test_code = """
import luigi
import gokart


class MyTask(gokart.TaskOnKart[str]):
    foo: luigi.IntParameter = luigi.IntParameter()
    bar: luigi.Parameter = luigi.Parameter()
    baz: gokart.ExplicitBoolParameter = gokart.ExplicitBoolParameter()


# bar is missing but not an error, because it can be set by a config file.
MyTask(foo='1', baz='not bool', complete_check_at_run='not bool')
"""
        stdout, stderr, exitcode = self._run_mypy(test_code)
        self.assertEqual(exitcode, 1, f'mypy error not occurred:\nstdout: {stdout}\nstderr: {stderr}')
        self.assertIn('error: Argument "foo" to "MyTask" has incompatible type "str"; expected "int"  [arg-type]', stdout)
        self.assertIn('error: Argument "baz" to "MyTask" has incompatible type "str"; expected "bool"  [arg-type]', stdout)
        self.assertIn('error: Argument "complete_check_at_run" to "MyTask" has incompatible type "str"; expected "bool"  [arg-type]', stdout)
        self.assertIn('Found 3 errors in 1 file (checked 1 source file)', stdout)
