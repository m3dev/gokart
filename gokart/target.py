import hashlib
import os
import shutil
from abc import abstractmethod
from typing import Any, Optional

import luigi
import luigi.contrib.s3

from gokart.file_processor import FileProcessor, make_file_processor
from gokart.workspace_config import WorkspaceConfig
from gokart.zip_client import make_zip_client


class TargetOnKart(luigi.Target):
    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def load(self) -> Any:
        pass

    @abstractmethod
    def dump(self, obj) -> None:
        pass


class SingleFileTarget(TargetOnKart):
    def __init__(self, target: luigi.target.FileSystemTarget, processor: FileProcessor):
        self._target = target
        self._processor = processor

    def exists(self) -> bool:
        return self._target.exists()

    def load(self) -> Any:
        with self._target.open('r') as f:
            return self._processor.load(f)

    def dump(self, obj) -> None:
        with self._target.open('w') as f:
            self._processor.dump(obj, f)


class ModelTarget(TargetOnKart):
    def __init__(self, file_path: str, temporary_directory: str, load_function, save_function) -> None:
        self._zip_client = make_zip_client(file_path, temporary_directory)
        self._temporary_directory = temporary_directory
        self._save_function = save_function
        self._load_function = load_function

    def exists(self) -> bool:
        return self._zip_client.exists()

    def load(self) -> Any:
        self._zip_client.unpack_archive()
        self._load_function = self._load_function or make_target(self._load_function_path()).load()
        model = self._load_function(self._model_path())
        self._remove_temporary_directory()
        return model

    def dump(self, obj) -> None:
        self._make_temporary_directory()
        self._save_function(obj, self._model_path())
        make_target(self._load_function_path()).dump(self._load_function)
        self._zip_client.make_archive()
        self._remove_temporary_directory()

    def _model_path(self):
        return os.path.join(self._temporary_directory, 'model.pkl')

    def _load_function_path(self):
        return os.path.join(self._temporary_directory, 'load_function.pkl')

    def _remove_temporary_directory(self):
        shutil.rmtree(self._temporary_directory)

    def _make_temporary_directory(self):
        os.makedirs(self._temporary_directory, exist_ok=True)


def _make_file_system_target(file_path: str) -> luigi.target.FileSystemTarget:
    processor = make_file_processor(file_path)
    if file_path.startswith('s3://'):
        return luigi.contrib.s3.S3Target(file_path, client=WorkspaceConfig().get_s3_client(), format=processor.format())
    return luigi.LocalTarget(file_path, format=processor.format())


def _make_file_path(original_path: str, unique_id: Optional[str] = None) -> str:
    if unique_id is not None:
        [base, extension] = os.path.splitext(original_path)
        return base + '_' + unique_id + extension
    return original_path


def make_target(file_path: str, unique_id: Optional[str] = None) -> TargetOnKart:
    file_path = _make_file_path(file_path, unique_id)
    processor = make_file_processor(file_path)
    file_system_target = _make_file_system_target(file_path)
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
