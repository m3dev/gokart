from __future__ import annotations

import contextlib
import os
import shutil
import uuid
import zipfile
from abc import abstractmethod
from typing import IO


def unzip_file(fp: str | IO[bytes] | os.PathLike[str], extract_dir: str) -> None:
    zip_file = zipfile.ZipFile(fp)
    zip_file.extractall(extract_dir)
    zip_file.close()


class ZipClient:
    @abstractmethod
    def exists(self) -> bool:
        pass

    @abstractmethod
    def make_archive(self, metadata: dict[str, str] | None = None) -> None:
        """Archive the temporary directory to the file path. `metadata` is attached to the object where the storage supports it (only GCS)."""
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

    def make_archive(self, metadata: dict[str, str] | None = None) -> None:
        # Write next to the destination and rename it into place, so readers without a target lock see either the old or the new archive, never a partial one.
        # Resolve symlinks so the rename replaces the file they point to, as writing in place did, instead of the link itself.
        destination = os.path.realpath(self._file_path)
        extension = os.path.splitext(self._file_path)[1]
        # The name does not derive from the destination, so a long file name does not exceed the file system limit, and the dot hides leftovers from globs.
        temporary_base = os.path.join(os.path.dirname(destination), f'.gokart-tmp-{uuid.uuid4().hex}')
        temporary_file_path = temporary_base + extension
        try:
            shutil.make_archive(base_name=temporary_base, format=extension[1:], root_dir=self._temporary_directory)
            os.replace(temporary_file_path, destination)
        except BaseException:
            with contextlib.suppress(FileNotFoundError):
                os.remove(temporary_file_path)
            raise

    def unpack_archive(self) -> None:
        unzip_file(fp=self._file_path, extract_dir=self._temporary_directory)

    def remove(self) -> None:
        # Ignore a missing archive to match S3ZipClient/GCSZipClient, whose underlying clients return False instead of raising.
        try:
            os.remove(self._file_path)
        except FileNotFoundError:
            pass

    @property
    def path(self) -> str:
        return self._file_path
