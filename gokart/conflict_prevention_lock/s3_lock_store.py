from __future__ import annotations

import random
import time
from typing import Any, cast

from gokart.conflict_prevention_lock.object_storage_lock import ConditionalConflictError, StoredLock, lock_object_name

# 412: our precondition failed. 409 is different: another conditional write or delete to the same key was in flight, which AWS documents as retryable,
# so it must not be read as "someone else holds the lock" when we replace or delete.
_PRECONDITION_FAILED_CODES = frozenset({'PreconditionFailed'})
_CONFLICT_CODES = frozenset({'ConditionalRequestConflict'})
_MISSING_CODES = frozenset({'NoSuchKey', '404', 'NotFound'})
# S3-compatible stores that do not know the conditional headers answer like this instead of ignoring them.
_UNSUPPORTED_CODES = frozenset({'NotImplemented', 'InvalidArgument', 'MalformedHeader'})
# The retry after a 409 is delayed a little, so that it more likely lands after the competing request has finished.
_CONFLICT_RETRY_DELAY_RANGE_SECONDS = (0.05, 0.25)


def _sleep_before_conflict_retry() -> None:
    time.sleep(random.uniform(*_CONFLICT_RETRY_DELAY_RANGE_SECONDS))


class UnsupportedConditionalWritesError(Exception):
    """Raised when the object store rejects conditional writes (If-Match / If-None-Match)."""


def _error_code(error: Exception) -> str:
    response = cast(dict[str, Any], getattr(error, 'response', {}))
    return str(response.get('Error', {}).get('Code', ''))


class S3LockStore:
    """Conditional writes on S3: ``If-None-Match: *`` creates, ``If-Match: <ETag>`` replaces / deletes.

    The ETag may be derived from the body (an MD5 for single-part objects without SSE-KMS / SSE-C), so the lock body must change
    on every write, which ``ObjectStorageRunLock`` guarantees with a nonce.
    """

    def __init__(self, client: Any, bucket_name: str, prefix: str) -> None:
        self._client: Any = client
        self._bucket = bucket_name
        self._prefix = prefix

    def close(self) -> None:
        self._client.close()

    def _name(self, key: str) -> str:
        return lock_object_name(self._prefix, key)

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
        return self._put(key, body, treat_conflict_as_failure=True, IfNoneMatch='*')

    def replace_if_match(self, key: str, body: bytes, version: str) -> str | None:
        return self._put(key, body, treat_conflict_as_failure=False, IfMatch=version)

    def delete_if_match(self, key: str, version: str) -> bool:
        from botocore.exceptions import ClientError

        for attempt in range(2):
            try:
                self._client.delete_object(Bucket=self._bucket, Key=self._name(key), IfMatch=version)
            except ClientError as e:
                code = _error_code(e)
                if code in _PRECONDITION_FAILED_CODES or code in _MISSING_CODES:
                    return False
                if code in _CONFLICT_CODES:
                    if attempt == 0:
                        _sleep_before_conflict_retry()
                        continue
                    raise ConditionalConflictError(f'{self._name(key)} kept conflicting with another conditional request.') from e
                self._raise_if_unsupported(code, e)
                raise
            return True
        raise AssertionError('unreachable')

    def _put(self, key: str, body: bytes, treat_conflict_as_failure: bool, **condition: str) -> str | None:
        from botocore.exceptions import ClientError

        for attempt in range(2):
            try:
                response = self._client.put_object(Bucket=self._bucket, Key=self._name(key), Body=body, ContentType='application/json', **condition)
            except ClientError as e:
                code = _error_code(e)
                if code in _PRECONDITION_FAILED_CODES or code in _MISSING_CODES:
                    return None
                if code in _CONFLICT_CODES:
                    # Creating: whoever is writing the same key at the same time holds the lock. Replacing: retry once with the same ETag, where a real loss
                    # shows up as 412; if it conflicts again, the heartbeat keeps its version and tries again on the next tick, and a takeover stays parked.
                    if treat_conflict_as_failure:
                        return None
                    if attempt == 0:
                        _sleep_before_conflict_retry()
                        continue
                    raise ConditionalConflictError(f'{self._name(key)} kept conflicting with another conditional request.') from e
                self._raise_if_unsupported(code, e)
                raise
            return str(response['ETag'])
        raise AssertionError('unreachable')

    @staticmethod
    def _raise_if_unsupported(code: str, error: Exception) -> None:
        if code in _UNSUPPORTED_CODES:
            raise UnsupportedConditionalWritesError(
                f'The object store rejected a conditional request ({code}). The S3 run lock needs conditional writes and deletes (If-Match / If-None-Match).'
            ) from error
