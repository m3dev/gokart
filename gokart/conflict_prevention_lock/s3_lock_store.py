from __future__ import annotations

from typing import Any, cast

from gokart.conflict_prevention_lock.object_storage_lock import StoredLock

# 412: our precondition failed, 409: another conditional write to the same key is in flight. Both mean someone else holds the lock.
_PRECONDITION_FAILED_CODES = frozenset({'PreconditionFailed', 'ConditionalRequestConflict'})
_MISSING_CODES = frozenset({'NoSuchKey', '404', 'NotFound'})
# S3-compatible stores that do not know the conditional headers answer like this instead of ignoring them.
_UNSUPPORTED_CODES = frozenset({'NotImplemented', 'InvalidArgument', 'MalformedHeader'})


class UnsupportedConditionalWritesError(Exception):
    """Raised when the object store rejects conditional writes (If-Match / If-None-Match)."""


def _error_code(error: Exception) -> str:
    response = cast(dict[str, Any], getattr(error, 'response', {}))
    return str(response.get('Error', {}).get('Code', ''))


class S3LockStore:
    """Conditional writes on S3: ``If-None-Match: *`` creates, ``If-Match: <ETag>`` replaces / deletes.

    The ETag is the MD5 of the body, so the lock body must change on every write (see ``ObjectStorageRunLock``).
    """

    def __init__(self, client: Any, bucket_name: str, prefix: str) -> None:
        self._client: Any = client
        self._bucket = bucket_name
        self._prefix = prefix.strip('/')

    def close(self) -> None:
        self._client.close()

    def _name(self, key: str) -> str:
        return f'{self._prefix}/{key}.lock' if self._prefix else f'{key}.lock'

    def stat(self, key: str) -> StoredLock | None:
        from botocore.exceptions import ClientError

        try:
            response = self._client.get_object(Bucket=self._bucket, Key=self._name(key))
        except ClientError as e:
            if _error_code(e) in _MISSING_CODES:
                return None
            raise
        return StoredLock(version=str(response['ETag']), updated_at=response['LastModified'].timestamp(), body=response['Body'].read())

    def create_if_absent(self, key: str, body: bytes) -> str | None:
        return self._put(key, body, IfNoneMatch='*')

    def replace_if_match(self, key: str, body: bytes, version: str) -> str | None:
        return self._put(key, body, IfMatch=version)

    def delete_if_match(self, key: str, version: str) -> bool:
        from botocore.exceptions import ClientError

        try:
            self._client.delete_object(Bucket=self._bucket, Key=self._name(key), IfMatch=version)
        except ClientError as e:
            code = _error_code(e)
            if code in _PRECONDITION_FAILED_CODES or code in _MISSING_CODES:
                return False
            raise
        return True

    def _put(self, key: str, body: bytes, **condition: str) -> str | None:
        from botocore.exceptions import ClientError

        try:
            response = self._client.put_object(Bucket=self._bucket, Key=self._name(key), Body=body, ContentType='application/json', **condition)
        except ClientError as e:
            code = _error_code(e)
            if code in _PRECONDITION_FAILED_CODES or code in _MISSING_CODES:
                return None
            if code in _UNSUPPORTED_CODES:
                raise UnsupportedConditionalWritesError(
                    f'The object store rejected a conditional write ({code}). The S3 run lock needs conditional writes (If-Match / If-None-Match).'
                ) from e
            raise
        return str(response['ETag'])
