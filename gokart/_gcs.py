"""GCS file system and target built on google-cloud-storage, replacing luigi.contrib.gcs inside gokart."""

from __future__ import annotations

import contextlib
import io
import json
import mimetypes
import os
import re
import tempfile
from datetime import datetime
from logging import getLogger
from typing import Any, Final, cast
from urllib.parse import urlsplit

import luigi
import luigi.format
from luigi.format import FileWrapper, Format

from gokart.gcs_config import GCSConfig, import_storage
from gokart.required_task_output import RequiredTaskOutput
from gokart.utils import FlattenableItems

logger = getLogger(__name__)


def path_to_bucket_and_key(path: str) -> tuple[str, str]:
    scheme, netloc, key, _, _ = urlsplit(path)
    assert scheme == 'gs'
    return netloc, key[1:]


# GCS limits custom metadata to 8 KiB per object (keys plus values): https://cloud.google.com/storage/quotas#objects
_MAX_METADATA_SIZE: Final[int] = 8 * 1024
_LOG_PATH_PATTERN: Final = re.compile(r'^gs://.+?/log/(processing_time/|task_info/|task_log/|module_versions/|random_seed/|task_params/).+')


def needs_task_state_labels(path: str) -> bool:
    """Whether a dump to `path` gets task state labels.

    Callers use this to skip building task_params / required_task_outputs, which are costly and unused otherwise.
    """
    # WHY: callers may hand over a target whose path() is not a str (e.g. a bare Mock(spec=TargetOnKart) in downstream tests).
    if not isinstance(path, str):  # pyright: ignore[reportUnnecessaryIsInstance]
        return False
    return path.startswith('gs://') and _LOG_PATH_PATTERN.match(path) is None


def build_task_state_labels(
    task_params: dict[str, str] | None = None,
    custom_labels: dict[str, Any] | None = None,
    required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
) -> dict[str, str]:
    """Build the object metadata attached to a dumped object, trimmed to fit GCS's size limit."""
    # custom_labels come first so that they win over task_params on a key conflict: users search by the labels they provided
    labels_list = [_normalize_labels(custom_labels), _normalize_labels(task_params)]
    if required_task_outputs:
        labels_list.append({'__required_task_outputs': json.dumps(_serialize_required_task_outputs(required_task_outputs))})

    merged: dict[str, str] = {}
    for labels in labels_list:
        for name, value in labels.items():
            if len(value) == 0:
                logger.warning(f'value of label_name={name} is empty. So skip to add as a metadata.')
                continue
            if name in merged:
                logger.warning(f'label_name={name} is already seen. So skip to add as metadata.')
                continue
            merged[name] = value
    return _trim_to_metadata_size_limit(merged)


def _normalize_labels(labels: dict[str, Any] | None) -> dict[str, str]:
    return {str(key): str(value) for key, value in labels.items()} if labels else {}


def _serialize_required_task_outputs(required_task_outputs: FlattenableItems[RequiredTaskOutput]) -> FlattenableItems[str]:
    if isinstance(required_task_outputs, RequiredTaskOutput):
        return cast(FlattenableItems[str], required_task_outputs.serialize())
    elif isinstance(required_task_outputs, dict):
        return {k: _serialize_required_task_outputs(v) for k, v in required_task_outputs.items()}
    elif isinstance(required_task_outputs, list | tuple):
        return [_serialize_required_task_outputs(ro) for ro in required_task_outputs]
    else:
        raise TypeError(
            f'Unsupported type for required_task_outputs: {type(required_task_outputs)}. It should be RequiredTaskOutput, dict, or iterable of RequiredTaskOutput.'
        )


def _trim_to_metadata_size_limit(labels: dict[str, str]) -> dict[str, str]:
    """Drop labels from the end until the total size fits, so that the upload itself never fails on the metadata size."""

    def _size(name: str, value: str) -> int:
        return len(name.encode('utf-8')) + len(value.encode('utf-8'))

    trimmed = dict(labels)
    total = sum(_size(name, value) for name, value in trimmed.items())
    for name, value in reversed(tuple(labels.items())):
        if total <= _MAX_METADATA_SIZE:
            break
        del trimmed[name]
        total -= _size(name, value)
    return trimmed


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

    def put(self, filename: str, dest_path: str, metadata: dict[str, str] | None = None) -> None:
        from google.cloud.storage.retry import DEFAULT_RETRY  # pyright: ignore[reportMissingTypeStubs]

        # google-cloud-storage<3 does not retry uploads without a precondition; overwriting with the same content is idempotent for gokart.
        # filename is a randomly named temporary file, so the content type has to be guessed from the destination like luigi.contrib.gcs did
        blob = self._blob(dest_path)
        if metadata is not None:
            # sent in the same request as the content, so labels never need a separate GET and PATCH
            blob.metadata = metadata
        blob.upload_from_filename(filename, content_type=mimetypes.guess_type(dest_path)[0], retry=DEFAULT_RETRY)

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
    def __init__(self, path: str, client: GCSClient, metadata: dict[str, str] | None) -> None:
        self._client = client
        self._metadata = metadata
        super().__init__(path)

    def move_to_final_destination(self) -> None:
        self._client.put(self.tmp_path, cast(str, self.path), metadata=self._metadata)


class GCSTarget(luigi.target.FileSystemTarget):
    def __init__(self, path: str, format: Format | None = None) -> None:
        super().__init__(path)
        self.format: Format = format or luigi.format.get_default_format()
        self._fs = GCSClient()

    @property
    def fs(self) -> GCSClient:
        return self._fs

    def open(self, mode: str = 'r', metadata: dict[str, str] | None = None) -> Any:
        if mode == 'r':
            return cast(Any, self.format).pipe_reader(FileWrapper(io.BufferedReader(self._fs.download(self.path))))
        if mode == 'w':
            return cast(Any, self.format).pipe_writer(_AtomicGCSFile(self.path, self._fs, metadata))
        raise ValueError(f"Unsupported open mode '{mode}'")
