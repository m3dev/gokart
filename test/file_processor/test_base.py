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
import dill  # pyright: ignore[reportMissingTypeStubs]
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


class _NonSeekableWriter(BytesIO):
    def seekable(self) -> bool:
        return False


class _FullDiskWriter(BytesIO):
    def __init__(self) -> None:
        super().__init__()
        self.write_calls = 0

    def write(self, b: Any) -> int:
        self.write_calls += 1
        raise OSError(28, 'No space left on device')


class _WriteSizeRecorder(BytesIO):
    def __init__(self) -> None:
        super().__init__()
        self.write_sizes: list[int] = []

    def write(self, b: Any) -> int:
        self.write_sizes.append(memoryview(b).nbytes)
        return super().write(b)


def _with_lambda() -> list[Any]:
    # The C pickler writes the array before it fails on the lambda, which only dill can pickle.
    return [np.arange(1 << 16), lambda x: x + 1]


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

    def test_dump_and_load_payload_larger_than_unpickler_prefetch_with_local_file(self):
        # The C unpickler prefetches 128 KiB via peek() and reads larger unframed payloads straight into the result with readinto(), so this crosses both paths.
        var = {'array': np.arange(1 << 15), 'bytes': os.urandom(1 << 18)}
        processor = PickleFileProcessor()

        with tempfile.TemporaryDirectory() as temp_dir:
            local_target = LocalTarget(path=f'{temp_dir}/temp.pkl', format=processor.format())
            with local_target.open('w') as f:
                processor.dump(var, f)
            with local_target.open('r') as f:
                loaded = processor.load(f)

        np.testing.assert_array_equal(loaded['array'], var['array'])
        self.assertEqual(loaded['bytes'], var['bytes'])

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

        self.assertEqual(buffer.getvalue(), pickle.dumps(array, protocol=5))

    def test_dump_streams_to_the_file_instead_of_building_the_whole_pickle_in_memory(self):
        obj = [f'item{i}' for i in range(100_000)]
        file = _WriteSizeRecorder()

        PickleFileProcessor().dump(obj, file)

        self.assertGreater(len(file.write_sizes), 1)
        self.assertLess(max(file.write_sizes), len(file.getvalue()))

    def test_dump_falls_back_to_dill_without_leaving_a_partial_pickle(self):
        obj = _with_lambda()
        processor = PickleFileProcessor()

        with tempfile.TemporaryDirectory() as temp_dir:
            local_target = LocalTarget(path=f'{temp_dir}/temp.pkl', format=processor.format())
            with local_target.open('w') as f:
                processor.dump(obj, f)
            with open(f'{temp_dir}/temp.pkl', 'rb') as f:
                dumped = f.read()
            with local_target.open('r') as f:
                loaded = processor.load(f)

        self.assertEqual(dumped, dill.dumps(obj, protocol=5))
        np.testing.assert_array_equal(loaded[0], obj[0])
        self.assertEqual(loaded[1](1), 2)

    def test_dump_to_a_non_seekable_file(self):
        array = np.arange(3)
        with_lambda = _with_lambda()
        for name, obj, expected in [
            ('C pickler', array, pickle.dumps(array, protocol=5)),
            ('dill fallback', with_lambda, dill.dumps(with_lambda, protocol=5)),
        ]:
            with self.subTest(name):
                file = _NonSeekableWriter()

                PickleFileProcessor().dump(obj, file)

                self.assertEqual(file.getvalue(), expected)

    def test_dump_raises_a_write_error_without_retrying_with_dill(self):
        file = _FullDiskWriter()

        with self.assertRaises(OSError):
            PickleFileProcessor().dump([np.arange(1 << 16)], file)

        self.assertEqual(file.write_calls, 1)

    def test_read_only_ndarray_loads_writable(self):
        array = np.arange(3)
        array.flags.writeable = False

        loaded = _dump_and_load({'array': array})

        np.testing.assert_array_equal(loaded['array'], array)
        self.assertTrue(loaded['array'].flags.writeable)

    def test_load_pickles_dumped_with_protocol_4(self):
        # files written before the switch to protocol 5, by the C pickler and by dill
        processor = PickleFileProcessor()

        loaded = processor.load(BytesIO(pickle.dumps({'array': np.arange(3)}, protocol=4)))
        loaded_by_dill = processor.load(BytesIO(dill.dumps([1, lambda x: x + 1], protocol=4)))

        np.testing.assert_array_equal(loaded['array'], np.arange(3))
        self.assertEqual(loaded_by_dill[1](1), 2)

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
