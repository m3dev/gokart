import os
import shutil
import zipfile
from abc import abstractmethod
from typing import IO, Union


def _unzip_file(fp: Union[str, IO, os.PathLike], extract_dir: str) -> None:
    zip_file = zipfile.ZipFile(fp)
    zip_file.extractall(extract_dir)
    zip_file.close()


class ZipClient(object):

    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def make_archive(self) -> None:
        pass

    @abstractmethod
    def unpack_archive(self) -> None:
        pass

    @abstractmethod
    def remove(self) -> None:
        pass

    @property
    @abstractmethod
    def path(self) -> str:
        pass


class LocalZipClient(ZipClient):

    def __init__(self, file_path: str, temporary_directory: str) -> None:
        self._file_path = file_path
        self._temporary_directory = temporary_directory

    def exists(self) -> bool:
        return os.path.exists(self._file_path)

    def make_archive(self) -> None:
        [base, extension] = os.path.splitext(self._file_path)
        shutil.make_archive(base_name=base, format=extension[1:], root_dir=self._temporary_directory)

    def unpack_archive(self) -> None:
        _unzip_file(fp=self._file_path, extract_dir=self._temporary_directory)

    def remove(self) -> None:
        shutil.rmtree(self._file_path, ignore_errors=True)

    @property
    def path(self) -> str:
        return self._file_path
