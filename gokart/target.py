import hashlib
import os
import pathlib
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
from gokart.zip_client_util import make_zip_client
from gokart.object_storage import ObjectStorage

logger = getLogger(__name__)


class TargetOnKart(luigi.Target):
    def exists(self) -> bool:
        return self._exists()

    def load(self) -> Any:
        return self._load()

    def dump(self, obj) -> None:
        self._dump(obj)

    def remove(self) -> None:
        self._remove()

    def last_modification_time(self) -> datetime:
        return self._last_modification_time()

    def path(self) -> str:
        return self._path()

    @abstractmethod
    def _exists(self) -> bool:
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
    def __init__(self, target: luigi.target.FileSystemTarget, processor: FileProcessor):
        self._target = target
        self._processor = processor

    def _exists(self) -> bool:
        return self._target.exists()

    def _load(self) -> Any:
        with self._target.open('r') as f:
            return self._processor.load(f)

    def _dump(self, obj) -> None:
        with self._target.open('w') as f:
            self._processor.dump(obj, f)

    def _remove(self) -> None:
        if self._target.exists():
            self._target.remove()

    def _last_modification_time(self) -> datetime:
        return _get_last_modification_time(self._target.path)

    def _path(self) -> str:
        return self._target.path


class ModelTarget(TargetOnKart):
    def __init__(self, file_path: str, temporary_directory: str, load_function, save_function) -> None:
        self._zip_client = make_zip_client(file_path, temporary_directory)
        self._temporary_directory = temporary_directory
        self._save_function = save_function
        self._load_function = load_function

    def _exists(self) -> bool:
        return self._zip_client.exists()

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

    def save(self, target_object: object, file_path: str):
        dir_path: pathlib.Path = pathlib.Path(file_path).parent
        self.save_recursively(target_object, dir_path)

    def save_recursively(self, target_object: object, file_path: pathlib.Path):
        if isinstance(target_object, pd.DataFrame):
            self._save_dataframe(target_object, dir_path=file_path)
        elif isinstance(target_object, list):
            for i, target_object_each in enumerate(target_object):
                self.save_recursively(target_object_each, file_path=file_path / str(i))
        elif isinstance(target_object, dict):
            for key, target_object_each in target_object.items():
                assert type(key) == str, ValueError('LargeDataFrameProcessor cannot handle dict with not string typed keys.')
                self.save_recursively(target_object_each, file_path=file_path / key)
        else:
            raise ValueError('target_object must be DataFrame or list or dict.')

    def _save_dataframe(self, df: pd.DataFrame, dir_path: pathlib.Path):
        dir_path.mkdir(exist_ok=True, parents=True)
        if df.empty:
            df.to_pickle(str(dir_path / 'data_0.pkl'))
            return

        split_size = df.values.nbytes // self.max_byte + 1
        logger.info(f'saving a large pdDataFrame with split_size={split_size}')
        for i, idx in tqdm(list(enumerate(np.array_split(range(df.shape[0]), split_size)))):
            df.iloc[idx[0]:idx[-1] + 1].to_pickle((dir_path / f'data_{i}.pkl'))

    @staticmethod
    def load(file_path: str) -> object:
        dir_path = pathlib.Path(file_path).parent
        return LargeDataFrameProcessor.load_recursively(dir_path=dir_path)


    @staticmethod
    def load_recursively(dir_path: pathlib.Path):
        print(f'load: {dir_path}')
        if (dir_path / 'data_0.pkl').exists():
            for file_path in dir_path.glob('data_*.pkl'):
                print(f'load_pickle: {file_path}')

            return pd.concat([pd.read_pickle(str(file_path)) for file_path in dir_path.glob('data_*.pkl')])
        elif (dir_path / '0').exists():
            def _load_list():
                i = 0
                while (dir_path / str(i)).exists():
                    yield LargeDataFrameProcessor.load_recursively(dir_path / str(i))
                    i += 1
            return list(_load_list())
        else:
            def _load_dict():
                for path in dir_path.iterdir():
                    assert path.is_dir()
                    yield path.name, LargeDataFrameProcessor.load_recursively(path)
            return dict(_load_dict())


def _make_file_system_target(file_path: str,
                             processor: Optional[FileProcessor] = None) -> luigi.target.FileSystemTarget:
    processor = processor or make_file_processor(file_path)
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


def make_target(file_path: str, unique_id: Optional[str] = None,
                processor: Optional[FileProcessor] = None) -> TargetOnKart:
    file_path = _make_file_path(file_path, unique_id)
    processor = processor or make_file_processor(file_path)
    file_system_target = _make_file_system_target(file_path, processor=processor)
    return SingleFileTarget(target=file_system_target, processor=processor)


def make_model_target(file_path: str,
                      temporary_directory: str,
                      save_function,
                      load_function,
                      unique_id: Optional[str] = None) -> TargetOnKart:
    file_path = _make_file_path(file_path, unique_id)
    temporary_directory = os.path.join(temporary_directory, hashlib.md5(file_path.encode()).hexdigest())
    return ModelTarget(
        file_path=file_path,
        temporary_directory=temporary_directory,
        save_function=save_function,
        load_function=load_function)
