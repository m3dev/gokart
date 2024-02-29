import os
import shutil

from gokart.gcs_config import GCSConfig
from gokart.zip_client import ZipClient, _unzip_file


class GCSZipClient(ZipClient):
    def __init__(self, file_path: str, temporary_directory: str) -> None:
        self._file_path = file_path
        self._temporary_directory = temporary_directory
        self._client = GCSConfig().get_gcs_client()

    def exists(self) -> bool:
        return self._client.exists(self._file_path)

    def make_archive(self) -> None:
        extension = os.path.splitext(self._file_path)[1]
        shutil.make_archive(base_name=self._temporary_directory, format=extension[1:], root_dir=self._temporary_directory)
        self._client.put(self._temporary_file_path(), self._file_path)

    def unpack_archive(self) -> None:
        os.makedirs(self._temporary_directory, exist_ok=True)
        file_pointer = self._client.download(self._file_path)
        _unzip_file(fp=file_pointer, extract_dir=self._temporary_directory)

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
