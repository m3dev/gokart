from __future__ import annotations

from typing import Any, cast

from gokart.conflict_prevention_lock.lock_config import LOCK_REQUEST_RETRY_DEADLINE_SECONDS, LOCK_REQUEST_TIMEOUT_SECONDS
from gokart.conflict_prevention_lock.object_storage_lock import StoredLock


class GCSLockStore:
    """Conditional writes on GCS: ``ifGenerationMatch=0`` creates, ``ifGenerationMatch=<generation>`` replaces / deletes."""

    def __init__(self, client: Any, bucket_name: str, prefix: str) -> None:
        self._client = client
        self._bucket: Any = client.bucket(bucket_name)
        self._prefix = prefix.strip('/')

    def close(self) -> None:
        self._client.close()

    @staticmethod
    def _request_options() -> dict[str, Any]:
        from google.cloud.storage.retry import DEFAULT_RETRY  # pyright: ignore[reportMissingTypeStubs]

        return {'timeout': LOCK_REQUEST_TIMEOUT_SECONDS, 'retry': cast(Any, DEFAULT_RETRY).with_deadline(LOCK_REQUEST_RETRY_DEADLINE_SECONDS)}

    def _name(self, key: str) -> str:
        return f'{self._prefix}/{key}.lock' if self._prefix else f'{key}.lock'

    def stat(self, key: str) -> StoredLock | None:
        from google.api_core import exceptions

        blob: Any = self._bucket.get_blob(self._name(key), **self._request_options())
        if blob is None:
            return None
        try:
            # Reading the generation that stat saw keeps the version and the body consistent.
            body = cast(bytes, self._bucket.blob(self._name(key), generation=blob.generation).download_as_bytes(**self._request_options()))
        except exceptions.NotFound:
            return None
        return StoredLock(version=str(blob.generation), updated_at=cast(float, blob.updated.timestamp()), body=body)

    def create_if_absent(self, key: str, body: bytes) -> str | None:
        return self._upload(key, body, if_generation_match=0)

    def replace_if_match(self, key: str, body: bytes, version: str) -> str | None:
        return self._upload(key, body, if_generation_match=int(version))

    def delete_if_match(self, key: str, version: str) -> bool:
        from google.api_core import exceptions

        try:
            self._bucket.blob(self._name(key)).delete(if_generation_match=int(version), **self._request_options())
        except (exceptions.PreconditionFailed, exceptions.NotFound):
            return False
        return True

    def _upload(self, key: str, body: bytes, if_generation_match: int) -> str | None:
        from google.api_core import exceptions

        blob: Any = self._bucket.blob(self._name(key))
        try:
            blob.upload_from_string(body, content_type='application/json', if_generation_match=if_generation_match, **self._request_options())
        except exceptions.PreconditionFailed:
            return None
        # NotFound is deliberately not caught here: on an upload it means that the bucket does not exist (a missing object fails with 412),
        # and swallowing it would make a typo in the url look like a lock held by someone else forever.
        return str(blob.generation)
