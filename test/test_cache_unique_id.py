import os
import unittest
from typing import Any

import luigi
import luigi.mock

import gokart


class _DummyTask(gokart.TaskOnKart[Any]):
    def requires(self):
        return _DummyTaskDep()

    def run(self):
        self.dump(self.load())


class _DummyTaskDep(gokart.TaskOnKart[str]):
    param: luigi.Parameter = luigi.Parameter()

    def run(self):
        self.dump(self.param)


class CacheUniqueIDTest(unittest.TestCase):
    def setUp(self):
        luigi.configuration.LuigiConfigParser._instance = None
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

    @staticmethod
    def _set_param(cls, attr_name: str, param: luigi.Parameter) -> None:  # type: ignore
        # Luigi 3.8.0+ uses __set_name__ to register _attribute_name on Parameter descriptors.
        # When assigning after class creation (bypassing the metaclass), call it manually.
        param.__set_name__(cls, attr_name)
        setattr(cls, attr_name, param)

    def test_cache_unique_id_true(self):
        self._set_param(_DummyTaskDep, 'param', luigi.Parameter(default='original_param'))

        output1 = gokart.build(_DummyTask(cache_unique_id=True), reset_register=False)

        self._set_param(_DummyTaskDep, 'param', luigi.Parameter(default='updated_param'))
        output2 = gokart.build(_DummyTask(cache_unique_id=True), reset_register=False)
        self.assertEqual(output1, output2)

    def test_cache_unique_id_false(self):
        self._set_param(_DummyTaskDep, 'param', luigi.Parameter(default='original_param'))

        output1 = gokart.build(_DummyTask(cache_unique_id=False), reset_register=False)

        self._set_param(_DummyTaskDep, 'param', luigi.Parameter(default='updated_param'))
        output2 = gokart.build(_DummyTask(cache_unique_id=False), reset_register=False)
        self.assertNotEqual(output1, output2)


if __name__ == '__main__':
    unittest.main()
