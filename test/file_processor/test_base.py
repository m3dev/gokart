"""Tests for base file processors (non-DataFrame processors)."""

from __future__ import annotations

import os
import pickle
import subprocess
import sys
import tempfile
import textwrap
import unittest
from collections.abc import Callable
from io import BytesIO
from typing import Any

import boto3
import numpy as np
from luigi import LocalTarget
from moto import mock_aws

from gokart.file_processor import PickleFileProcessor
from gokart.object_storage import ObjectStorage


class _TaggedArray(np.ndarray[Any, Any]):
    unit: str


def _answer(self: _Holder) -> int:
    return 42


class _Holder:
    answer = _answer


def _dump_and_load(obj: Any) -> Any:
    processor = PickleFileProcessor()
    buffer = BytesIO()
    processor.dump(obj, buffer)
    buffer.seek(0)
    return processor.load(buffer)


class TestPickleFileProcessor(unittest.TestCase):
    def test_dump_and_load_normal_obj(self):
        var = 'abc'
        processor = PickleFileProcessor()

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.pkl'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(var, f)
            with local_target.open('r') as f:
                loaded = processor.load(f)

        self.assertEqual(loaded, var)

    def test_dump_and_load_class(self):
        import functools

        def plus1(func: Callable[..., int]) -> Callable[..., int]:
            @functools.wraps(func)
            def wrapped() -> int:
                ret = func()
                return ret + 1

            return wrapped

        class A:
            def __init__(self) -> None:
                self.run = plus1(self.run)

            def run(self) -> int:
                return 1

        obj = A()
        processor = PickleFileProcessor()
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = f'{temp_dir}/temp.pkl'
            local_target = LocalTarget(path=temp_path, format=processor.format())
            with local_target.open('w') as f:
                processor.dump(obj, f)
            with local_target.open('r') as f:
                loaded = processor.load(f)

        self.assertEqual(loaded.run(), obj.run())

    def test_dump_ndarray_with_standard_pickle(self):
        array = np.arange(3)
        buffer = BytesIO()

        PickleFileProcessor().dump(array, buffer)

        self.assertEqual(buffer.getvalue(), pickle.dumps(array, protocol=4))

    def test_dump_and_load_object_defined_in_main_module(self):
        dump_script = textwrap.dedent(
            """
            import functools
            import sys

            from luigi import LocalTarget

            from gokart.file_processor import PickleFileProcessor


            class Point:
                def __init__(self, x):
                    self.x = x


            def double(v):
                return v * 2


            @functools.lru_cache
            def triple(v):
                return v * 3


            processor = PickleFileProcessor()
            for name, obj in {'point': Point(3), 'double': double, 'triple': triple}.items():
                with LocalTarget(path=f'{sys.argv[1]}/{name}.pkl', format=processor.format()).open('w') as f:
                    processor.dump(obj, f)
            """
        )
        processor = PickleFileProcessor()
        loaded = {}
        with tempfile.TemporaryDirectory() as temp_dir:
            subprocess.run([sys.executable, '-c', dump_script, temp_dir], check=True)
            for name in ['point', 'double', 'triple']:
                with LocalTarget(path=f'{temp_dir}/{name}.pkl', format=processor.format()).open('r') as f:
                    loaded[name] = processor.load(f)

        self.assertEqual([loaded['point'].x, loaded['double'](1), loaded['triple'](1)], [3, 2, 3])

    def test_dump_and_load_ndarray_subclass_with_attributes(self):
        array = np.arange(3).view(_TaggedArray)
        array.unit = 'cm'

        self.assertEqual(_dump_and_load(array).unit, 'cm')

    def test_dump_and_load_method_bound_under_another_name(self):
        self.assertEqual(_dump_and_load(_Holder().answer)(), 42)

    @mock_aws
    def test_dump_and_load_with_readables3file(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')
        file_path = os.path.join('s3://test/', 'test.pkl')

        var = 'abc'
        processor = PickleFileProcessor()

        target = ObjectStorage.get_object_storage_target(file_path, processor.format())
        # luigi.target.FileSystemTarget.open() has no annotation and its body is `pass`, so pyright infers its return type as None.
        with target.open('w') as f:  # pyright: ignore[reportGeneralTypeIssues]
            processor.dump(var, f)
        with target.open('r') as f:  # pyright: ignore[reportGeneralTypeIssues]
            loaded = processor.load(f)

        self.assertEqual(loaded, var)
