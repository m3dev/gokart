"""GCS file system and target built on google-cloud-storage, replacing luigi.contrib.gcs inside gokart."""

from __future__ import annotations

import contextlib
import io
import mimetypes
import os
import tempfile
from datetime import datetime
from typing import Any, cast
from urllib.parse import urlsplit

import luigi
import luigi.format
from luigi.format import FileWrapper, Format

from gokart.gcs_config import GCSConfig, import_storage


def path_to_bucket_and_key(path: str) -> tuple[str, str]:
    scheme, netloc, key, _, _ = urlsplit(path)
    assert scheme == 'gs'
    return netloc, key[1:]


class _DeleteOnCloseFile(io.FileIO):
    def close(self) -> None:
        super().close()
        try:
            os.remove(self.name)
        except OSError:
            pass


class GCSClient:
    """The subset of luigi.target.FileSystem that gokart uses. Only covers single objects; gokart never treats gs:// paths as directories."""

    def __init__(self) -> None:
        # fail when the target is built, not on first I/O, so a missing extra is reported where the gs:// path is configured
        import_storage()

    def _bucket(self, bucket: str) -> Any:
        # resolved on every call so that a forked worker gets its own client (see GCSConfig.get_storage_client)
        return cast(Any, GCSConfig().get_storage_client()).bucket(bucket)

    def _blob(self, path: str) -> Any:
        bucket, key = path_to_bucket_and_key(path)
        return self._bucket(bucket).blob(key)

    def exists(self, path: str) -> bool:
        return cast(bool, self._blob(path).exists())

    def remove(self, path: str, recursive: bool = True, skip_trash: bool = True) -> bool:
        from google.api_core.exceptions import NotFound

        try:
            self._blob(path).delete()
        except NotFound:
            return False
        return True

    def put(self, filename: str, dest_path: str) -> None:
        from google.cloud.storage.retry import DEFAULT_RETRY  # pyright: ignore[reportMissingTypeStubs]

        # google-cloud-storage<3 does not retry uploads without a precondition; overwriting with the same content is idempotent for gokart.
        # filename is a randomly named temporary file, so the content type has to be guessed from the destination like luigi.contrib.gcs did
        self._blob(dest_path).upload_from_filename(filename, content_type=mimetypes.guess_type(dest_path)[0], retry=DEFAULT_RETRY)

    def get_timestamp(self, path: str) -> datetime:
        bucket, key = path_to_bucket_and_key(path)
        blob = self._bucket(bucket).get_blob(key)
        if blob is None:
            raise FileNotFoundError(f'No such object: {path}')
        return cast(datetime, blob.updated)

    def download(self, path: str) -> io.FileIO:
        # stream to a temporary file instead of memory, since outputs can be large
        with tempfile.NamedTemporaryFile(delete=False) as fp:
            try:
                self._blob(path).download_to_file(fp)
            except BaseException:
                fp.close()
                # a cleanup failure must not mask the download error
                with contextlib.suppress(OSError):
                    os.remove(fp.name)
                raise
        return _DeleteOnCloseFile(fp.name, 'r')


class _AtomicGCSFile(luigi.target.AtomicLocalFile):
    def __init__(self, path: str, client: GCSClient) -> None:
        self._client = client
        super().__init__(path)

    def move_to_final_destination(self) -> None:
        self._client.put(self.tmp_path, cast(str, self.path))


class GCSTarget(luigi.target.FileSystemTarget):
    def __init__(self, path: str, format: Format | None = None) -> None:
        super().__init__(path)
        self.format: Format = format or luigi.format.get_default_format()
        self._fs = GCSClient()

    @property
    def fs(self) -> GCSClient:
        return self._fs

    def open(self, mode: str = 'r') -> Any:
        if mode == 'r':
            return cast(Any, self.format).pipe_reader(FileWrapper(io.BufferedReader(self._fs.download(self.path))))
        if mode == 'w':
            return cast(Any, self.format).pipe_writer(_AtomicGCSFile(self.path, self._fs))
        raise ValueError(f"Unsupported open mode '{mode}'")
