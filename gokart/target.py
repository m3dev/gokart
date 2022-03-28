import hashlib
import os
import shutil
from abc import abstractmethod
from datetime import datetime
from glob import glob
from logging import getLogger
from typing import Any, Optional

import luigi
import numpy as np
import pandas as pd
from tqdm import tqdm

from gokart.file_processor import FileProcessor, make_file_processor
from gokart.object_storage import ObjectStorage
from gokart.redis_lock import RedisParams, make_redis_params, wrap_with_dump_lock, wrap_with_load_lock, wrap_with_remove_lock, wrap_with_run_lock
from gokart.zip_client_util import make_zip_client

logger = getLogger(__name__)


class TargetOnKart(luigi.Target):

    def exists(self) -> bool:
        return self._exists()

    def load(self) -> Any:
        return wrap_with_load_lock(func=self._load, redis_params=self._get_redis_params())()

    def dump(self, obj, lock_at_dump: bool = True) -> None:
        if lock_at_dump:
            wrap_with_dump_lock(func=self._dump, redis_params=self._get_redis_params(), exist_check=self.exists)(obj)
        else:
            self._dump(obj)

    def remove(self) -> None:
        if self.exists():
            wrap_with_remove_lock(self._remove, redis_params=self._get_redis_params())()

    def last_modification_time(self) -> datetime:
        return self._last_modification_time()

    def path(self) -> str:
        return self._path()

    def wrap_with_lock(self, func):
        return wrap_with_run_lock(func=func, redis_params=self._get_redis_params())

    @abstractmethod
    def _exists(self) -> bool:
        pass

    @abstractmethod
    def _get_redis_params(self) -> RedisParams:
        pass

    @abstractmethod
    def _load(self) -> Any:
        pass

    @abstractmethod
    def _dump(self, obj) -> None:
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
        redis_params: RedisParams,
    ) -> None:
        self._target = target
        self._processor = processor
        self._redis_params = redis_params

    def _exists(self) -> bool:
        return self._target.exists()

    def _get_redis_params(self) -> RedisParams:
        return self._redis_params

    def _load(self) -> Any:
        with self._target.open('r') as f:
            return self._processor.load(f)

    def _dump(self, obj) -> None:
        with self._target.open('w') as f:
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
        load_function,
        save_function,
        redis_params: RedisParams,
    ) -> None:
        self._zip_client = make_zip_client(file_path, temporary_directory)
        self._temporary_directory = temporary_directory
        self._save_function = save_function
        self._load_function = load_function
        self._redis_params = redis_params

    def _exists(self) -> bool:
        return self._zip_client.exists()

    def _get_redis_params(self) -> RedisParams:
        return self._redis_params

    def _load(self) -> Any:
        self._zip_client.unpack_archive()
        self._load_function = self._load_function or make_target(self._load_function_path()).load()
        model = self._load_function(self._model_path())
        self._remove_temporary_directory()
        return model

    def _dump(self, obj) -> None:
        self._make_temporary_directory()
        self._save_function(obj, self._model_path())
        make_target(self._load_function_path()).dump(self._load_function)
        self._zip_client.make_archive()
        self._remove_temporary_directory()

    def _remove(self) -> None:
        self._zip_client.remove()

    def _last_modification_time(self) -> datetime:
        return _get_last_modification_time(self._zip_client.path)

    def _path(self) -> str:
        return self._zip_client.path

    def _model_path(self):
        return os.path.join(self._temporary_directory, 'model.pkl')

    def _load_function_path(self):
        return os.path.join(self._temporary_directory, 'load_function.pkl')

    def _remove_temporary_directory(self):
        shutil.rmtree(self._temporary_directory)

    def _make_temporary_directory(self):
        os.makedirs(self._temporary_directory, exist_ok=True)


class LargeDataFrameProcessor(object):

    def __init__(self, max_byte: int):
        self.max_byte = int(max_byte)

    def save(self, df: pd.DataFrame, file_path: str):
        dir_path = os.path.dirname(file_path)
        os.makedirs(dir_path, exist_ok=True)

        if df.empty:
            df.to_pickle(os.path.join(dir_path, 'data_0.pkl'))
            return

        split_size = df.values.nbytes // self.max_byte + 1
        logger.info(f'saving a large pdDataFrame with split_size={split_size}')
        for i, idx in tqdm(list(enumerate(np.array_split(range(df.shape[0]), split_size)))):
            df.iloc[idx[0]:idx[-1] + 1].to_pickle(os.path.join(dir_path, f'data_{i}.pkl'))

    @staticmethod
    def load(file_path: str) -> pd.DataFrame:
        dir_path = os.path.dirname(file_path)

        return pd.concat([pd.read_pickle(file_path) for file_path in glob(os.path.join(dir_path, 'data_*.pkl'))])


def _make_file_system_target(file_path: str, processor: Optional[FileProcessor] = None, store_index_in_feather: bool = True) -> luigi.target.FileSystemTarget:
    processor = processor or make_file_processor(file_path, store_index_in_feather=store_index_in_feather)
    if ObjectStorage.if_object_storage_path(file_path):
        return ObjectStorage.get_object_storage_target(file_path, processor.format())
    return luigi.LocalTarget(file_path, format=processor.format())


def _make_file_path(original_path: str, unique_id: Optional[str] = None) -> str:
    if unique_id is not None:
        [base, extension] = os.path.splitext(original_path)
        return base + '_' + unique_id + extension
    return original_path


def _get_last_modification_time(path: str) -> datetime:
    if ObjectStorage.if_object_storage_path(path):
        if ObjectStorage.exists(path):
            return ObjectStorage.get_timestamp(path)
        raise FileNotFoundError(f'No such file or directory: {path}')
    return datetime.fromtimestamp(os.path.getmtime(path))


def make_target(file_path: str,
                unique_id: Optional[str] = None,
                processor: Optional[FileProcessor] = None,
                redis_params: RedisParams = None,
                store_index_in_feather: bool = True) -> TargetOnKart:
    _redis_params = redis_params if redis_params is not None else make_redis_params(file_path=file_path, unique_id=unique_id)
    file_path = _make_file_path(file_path, unique_id)
    processor = processor or make_file_processor(file_path, store_index_in_feather=store_index_in_feather)
    file_system_target = _make_file_system_target(file_path, processor=processor, store_index_in_feather=store_index_in_feather)
    return SingleFileTarget(target=file_system_target, processor=processor, redis_params=_redis_params)


def make_model_target(file_path: str,
                      temporary_directory: str,
                      save_function,
                      load_function,
                      unique_id: Optional[str] = None,
                      redis_params: RedisParams = None) -> TargetOnKart:
    _redis_params = redis_params if redis_params is not None else make_redis_params(file_path=file_path, unique_id=unique_id)
    file_path = _make_file_path(file_path, unique_id)
    temporary_directory = os.path.join(temporary_directory, hashlib.md5(file_path.encode()).hexdigest())
    return ModelTarget(file_path=file_path,
                       temporary_directory=temporary_directory,
                       save_function=save_function,
                       load_function=load_function,
                       redis_params=_redis_params)
