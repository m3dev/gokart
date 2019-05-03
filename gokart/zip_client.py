import os
import shutil
import zipfile
from abc import abstractmethod

from gokart.s3_config import S3Config


def _unzip_file(filename: str, extract_dir: str) -> None:
    zip_file = zipfile.ZipFile(filename)
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
        _unzip_file(filename=self._file_path, extract_dir=self._temporary_directory)

    def remove(self) -> None:
        shutil.rmtree(self._file_path, ignore_errors=True)

    @property
    def path(self) -> str:
        return self._file_path


class S3ZipClient(ZipClient):
    def __init__(self, file_path: str, temporary_directory: str) -> None:
        self._file_path = file_path
        self._temporary_directory = temporary_directory
        self._client = S3Config().get_s3_client()

    def exists(self) -> bool:
        return self._client.exists(self._file_path)

    def make_archive(self) -> None:
        extension = os.path.splitext(self._file_path)[1]
        shutil.make_archive(base_name=self._temporary_directory, format=extension[1:], root_dir=self._temporary_directory)
        self._client.put(self._temporary_file_path(), self._file_path)

    def unpack_archive(self) -> None:
        os.makedirs(self._temporary_directory, exist_ok=True)
        self._client.get(self._file_path, self._temporary_file_path())
        _unzip_file(filename=self._temporary_file_path(), extract_dir=self._temporary_directory)

    def remove(self) -> None:
        self._client.remove(self._file_path)

    @property
    def path(self) -> str:
        return self._file_path

    def _temporary_file_path(self):
        extension = os.path.splitext(self._file_path)[1]
        base_name = self._temporary_directory
        if base_name.endswith('/'):
            base_name = base_name[:-1]
        return base_name + extension


def make_zip_client(file_path: str, temporary_directory: str) -> ZipClient:
    if file_path.startswith('s3://'):
        return S3ZipClient(file_path=file_path, temporary_directory=temporary_directory)
    return LocalZipClient(file_path=file_path, temporary_directory=temporary_directory)
