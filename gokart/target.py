from __future__ import annotations

import hashlib
import os
import re
import shutil
import tempfile
from abc import abstractmethod
from collections.abc import Generator
from contextlib import contextmanager
from datetime import datetime
from glob import glob
from logging import getLogger
from typing import Any, cast

import luigi
import numpy as np
import pandas as pd

from gokart._gcs import GCSTarget, build_task_state_labels, needs_task_state_labels
from gokart.conflict_prevention_lock.task_lock import TaskLockParams, make_task_lock_params
from gokart.conflict_prevention_lock.task_lock_wrappers import wrap_dump_with_lock, wrap_load_with_lock, wrap_remove_with_lock
from gokart.file_processor import FileProcessor, make_file_processor
from gokart.object_storage import ObjectStorage
from gokart.required_task_output import RequiredTaskOutput
from gokart.utils import FlattenableItems
from gokart.zip_client_util import make_zip_client

logger = getLogger(__name__)


class TargetOnKart(luigi.Target):
    # luigi.Target.exists() has no annotation and its body is `pass`, so pyright infers its return type as None.
    def exists(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
        return self._exists()

    def load(self) -> Any:
        return wrap_load_with_lock(func=self._load, task_lock_params=self._get_task_lock_params())()

    def dump(
        self,
        obj: Any,
        lock_at_dump: bool = True,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> None:
        if lock_at_dump:
            wrap_dump_with_lock(func=self._dump, task_lock_params=self._get_task_lock_params(), exist_check=self.exists)(
                obj=obj,
                task_params=task_params,
                custom_labels=custom_labels,
                required_task_outputs=required_task_outputs,
            )
        else:
            self._dump(obj=obj, task_params=task_params, custom_labels=custom_labels, required_task_outputs=required_task_outputs)

    def remove(self) -> None:
        if self.exists():
            wrap_remove_with_lock(self._remove, task_lock_params=self._get_task_lock_params())()

    def last_modification_time(self) -> datetime:
        return self._last_modification_time()

    def path(self) -> str:
        return self._path()

    def is_persistent(self) -> bool:
        """Whether the output is stored at path().

        False for in-memory outputs (``TaskOnKart.make_target(..., persist=False)``), whose path() is only a key built like a file path.
        Overriding it does not make a target in-memory: only InMemoryTarget is kept in the store of gokart.build.
        """
        return True

    @abstractmethod
    def _exists(self) -> bool:
        pass

    @abstractmethod
    def _get_task_lock_params(self) -> TaskLockParams:
        pass

    @abstractmethod
    def _load(self) -> Any:
        pass

    @abstractmethod
    def _dump(
        self,
        obj: Any,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> None:
        pass

    @abstractmethod
    def _remove(self) -> None:
        pass

    @abstractmethod
    def _last_modification_time(self) -> datetime:
        pass

    @abstractmethod
    def _path(self) -> str:
        pass


class SingleFileTarget(TargetOnKart):
    def __init__(
        self,
        target: luigi.target.FileSystemTarget,
        processor: FileProcessor,
        task_lock_params: TaskLockParams,
    ) -> None:
        self._target = target
        self._processor = processor
        self._task_lock_params = task_lock_params

    def _exists(self) -> bool:
        return cast(bool, self._target.exists())

    def _get_task_lock_params(self) -> TaskLockParams:
        return self._task_lock_params

    def _load(self) -> Any:
        # luigi.target.FileSystemTarget.open() has no annotation and its body is `pass`, so pyright infers its return type as None.
        with cast(Any, self._target).open('r') as f:
            return self._processor.load(f)

    def _dump(
        self,
        obj: Any,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> None:
        needs_labels = needs_task_state_labels(self.path())
        # WHY: SingleFileTarget accepts any FileSystemTarget, so a gs:// path alone does not guarantee open() takes metadata.
        if needs_labels and isinstance(self._target, GCSTarget):
            # built before writing, so an invalid label input fails the dump without leaving an object behind
            labels = build_task_state_labels(task_params=task_params, custom_labels=custom_labels, required_task_outputs=required_task_outputs)
            file = self._target.open('w', metadata=labels)
        else:
            if needs_labels:
                logger.debug(f'skip task state labels for {self.path()}: {type(self._target).__name__} cannot attach object metadata')
            file = cast(Any, self._target).open('w')
        with file as f:
            self._processor.dump(obj, f)

    def _remove(self) -> None:
        self._target.remove()

    def _last_modification_time(self) -> datetime:
        return _get_last_modification_time(self._target.path)

    def _path(self) -> str:
        return self._target.path


class ModelTarget(TargetOnKart):
    def __init__(
        self,
        file_path: str,
        temporary_directory: str,
        load_function: Any,
        save_function: Any,
        task_lock_params: TaskLockParams,
    ) -> None:
        self._zip_client = make_zip_client(file_path, temporary_directory)
        self._temporary_directory = temporary_directory
        self._save_function = save_function
        self._load_function = load_function
        self._task_lock_params = task_lock_params

    def _exists(self) -> bool:
        return self._zip_client.exists()

    def _get_task_lock_params(self) -> TaskLockParams:
        return self._task_lock_params

    def _load(self) -> Any:
        with self._make_working_directory() as working_directory:
            make_zip_client(self._zip_client.path, working_directory).unpack_archive()
            self._load_function = self._load_function or make_target(self._load_function_path(working_directory)).load()
            return self._load_function(self._model_path(working_directory))

    def _dump(
        self,
        obj: Any,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> None:
        # The inner load_function.pkl is written locally, so the labels are attached to the uploaded zip itself.
        # Built before writing, so an invalid label input fails the dump without leaving an object behind.
        labels = (
            build_task_state_labels(task_params=task_params, custom_labels=custom_labels, required_task_outputs=required_task_outputs)
            if needs_task_state_labels(self._zip_client.path)
            else None
        )
        with self._make_working_directory() as working_directory:
            self._save_function(obj, self._model_path(working_directory))
            make_target(self._load_function_path(working_directory)).dump(self._load_function)
            make_zip_client(self._zip_client.path, working_directory).make_archive(metadata=labels)

    def _remove(self) -> None:
        self._zip_client.remove()

    def _last_modification_time(self) -> datetime:
        return _get_last_modification_time(self._zip_client.path)

    def _path(self) -> str:
        return self._zip_client.path

    @staticmethod
    def _model_path(working_directory: str) -> str:
        return os.path.join(working_directory, 'model.pkl')

    @staticmethod
    def _load_function_path(working_directory: str) -> str:
        return os.path.join(working_directory, 'load_function.pkl')

    @contextmanager
    def _make_working_directory(self) -> Generator[str]:
        # self._temporary_directory is the same for every call on the same file path, so each call gets its own directory.
        # Otherwise a call picks up files of a concurrent call or of a call that crashed before cleanup, and its cleanup removes files still in use.
        base = os.path.normpath(self._temporary_directory)
        os.makedirs(os.path.dirname(base), exist_ok=True)
        call_directory = tempfile.mkdtemp(prefix=f'{os.path.basename(base)}_', dir=os.path.dirname(base))
        try:
            # Object storage zip clients write the archive next to their directory, so nest it to remove the archive together.
            working_directory = os.path.join(call_directory, 'model')
            os.makedirs(working_directory)
            yield working_directory
        finally:
            shutil.rmtree(call_directory, ignore_errors=True)


_LARGE_DATA_FRAME_CHUNK_PATTERN = re.compile(r'data_(\d+)\.pkl')


class LargeDataFrameProcessor:
    def __init__(self, max_byte: int):
        self.max_byte = int(max_byte)
        if self.max_byte <= 0:
            raise ValueError(f'max_byte must be positive, got {max_byte}')

    def save(self, df: pd.DataFrame, file_path: str) -> None:
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)

        # load concatenates every data_*.pkl, so chunks left by an earlier, larger save would be mixed into this one.
        for _, stale_path in self._chunk_paths(dir_path):
            os.remove(stale_path)

        if df.empty:
            df.to_pickle(os.path.join(dir_path, 'data_0.pkl'))
            return

        # array_split yields empty chunks when split_size exceeds the row count, and idx[0] fails on them.
        split_size = min(df.values.nbytes // self.max_byte + 1, df.shape[0])
        logger.info(f'saving a large pdDataFrame with split_size={split_size}')
        for i, idx in enumerate(cast(Any, np).array_split(range(df.shape[0]), split_size)):
            df.iloc[idx[0] : idx[-1] + 1].to_pickle(os.path.join(dir_path, f'data_{i}.pkl'))

    @staticmethod
    def _chunk_paths(dir_path: str) -> list[tuple[int, str]]:
        # glob returns files in arbitrary order, and lexicographic order puts data_10 before data_2, so sort by chunk index.
        chunks: list[tuple[int, str]] = []
        for fp in glob(os.path.join(dir_path, 'data_*.pkl')):
            m = _LARGE_DATA_FRAME_CHUNK_PATTERN.fullmatch(os.path.basename(fp))
            if m is not None:
                chunks.append((int(m.group(1)), fp))
        return sorted(chunks)

    @staticmethod
    def load(file_path: str) -> pd.DataFrame:
        dir_path = os.path.dirname(file_path)
        return cast(pd.DataFrame, pd.concat([pd.read_pickle(fp) for _, fp in LargeDataFrameProcessor._chunk_paths(dir_path)]))


def is_persistent_target(target: luigi.Target) -> bool:
    """TargetOnKart.is_persistent() that also accepts plain luigi targets, which output() may return despite its annotation."""
    return not isinstance(target, TargetOnKart) or target.is_persistent()


def _make_file_system_target(file_path: str, processor: FileProcessor | None = None, store_index_in_feather: bool = True) -> luigi.target.FileSystemTarget:
    processor = processor or make_file_processor(file_path, store_index_in_feather=store_index_in_feather)
    if ObjectStorage.if_object_storage_path(file_path):
        return ObjectStorage.get_object_storage_target(file_path, processor.format())
    return luigi.LocalTarget(file_path, format=processor.format())


def _make_file_path(original_path: str, unique_id: str | None = None) -> str:
    if unique_id is not None:
        [base, extension] = os.path.splitext(original_path)
        return base + '_' + unique_id + extension
    return original_path


def _get_last_modification_time(path: str) -> datetime:
    if ObjectStorage.if_object_storage_path(path):
        # get_timestamp raises FileNotFoundError itself, so an extra exists() round trip is unnecessary
        return ObjectStorage.get_timestamp(path)
    return datetime.fromtimestamp(os.path.getmtime(path))


def make_target(
    file_path: str,
    unique_id: str | None = None,
    processor: FileProcessor | None = None,
    task_lock_params: TaskLockParams | None = None,
    store_index_in_feather: bool = True,
) -> TargetOnKart:
    _task_lock_params = task_lock_params if task_lock_params is not None else make_task_lock_params(file_path=file_path, unique_id=unique_id)
    file_path = _make_file_path(file_path, unique_id)
    processor = processor or make_file_processor(file_path, store_index_in_feather=store_index_in_feather)
    file_system_target = _make_file_system_target(file_path, processor=processor, store_index_in_feather=store_index_in_feather)
    return SingleFileTarget(target=file_system_target, processor=processor, task_lock_params=_task_lock_params)


def make_model_target(
    file_path: str,
    temporary_directory: str,
    save_function: Any,
    load_function: Any,
    unique_id: str | None = None,
    task_lock_params: TaskLockParams | None = None,
) -> TargetOnKart:
    _task_lock_params = task_lock_params if task_lock_params is not None else make_task_lock_params(file_path=file_path, unique_id=unique_id)
    file_path = _make_file_path(file_path, unique_id)
    temporary_directory = os.path.join(temporary_directory, hashlib.md5(file_path.encode()).hexdigest())
    return ModelTarget(
        file_path=file_path,
        temporary_directory=temporary_directory,
        save_function=save_function,
        load_function=load_function,
        task_lock_params=_task_lock_params,
    )
