from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import ANY, MagicMock

import pytest
from google.api_core import exceptions as gcs_exceptions

from gokart.conflict_prevention_lock.gcs_lock_store import GCSLockStore


def _blob(generation: int = 7, updated: datetime = datetime(2026, 11, 22, tzinfo=UTC)) -> MagicMock:
    blob = MagicMock()
    blob.generation = generation
    blob.updated = updated
    return blob


@pytest.fixture
def bucket() -> MagicMock:
    return MagicMock()


@pytest.fixture
def store(bucket: MagicMock) -> GCSLockStore:
    client = MagicMock()
    client.bucket.return_value = bucket
    return GCSLockStore(client, 'my-bucket', 'locks/run')


class TestObjectName:
    def test_object_is_named_prefix_key_lock(self, store: GCSLockStore, bucket: MagicMock):
        bucket.blob.return_value = _blob()
        store.create_if_absent('task_run', b'x')
        bucket.blob.assert_called_once_with('locks/run/task_run.lock')

    def test_empty_prefix_has_no_leading_slash(self, bucket: MagicMock):
        client = MagicMock()
        client.bucket.return_value = bucket
        bucket.blob.return_value = _blob()
        GCSLockStore(client, 'my-bucket', '').create_if_absent('task_run', b'x')
        bucket.blob.assert_called_once_with('task_run.lock')


class TestCreateIfAbsent:
    def test_uploads_with_generation_zero_and_returns_the_new_generation(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob(generation=42)
        bucket.blob.return_value = blob
        assert store.create_if_absent('k', b'body') == '42'
        blob.upload_from_string.assert_called_once()
        assert blob.upload_from_string.call_args.args[0] == b'body'
        assert blob.upload_from_string.call_args.kwargs['if_generation_match'] == 0

    def test_precondition_failure_returns_none(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob()
        blob.upload_from_string.side_effect = gcs_exceptions.PreconditionFailed('exists')
        bucket.blob.return_value = blob
        assert store.create_if_absent('k', b'body') is None

    def test_a_missing_bucket_is_not_mistaken_for_a_lock_held_by_someone_else(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob()
        blob.upload_from_string.side_effect = gcs_exceptions.NotFound('no such bucket')
        bucket.blob.return_value = blob
        with pytest.raises(gcs_exceptions.NotFound):
            store.create_if_absent('k', b'body')

    def test_requests_carry_an_explicit_timeout_and_retry_deadline(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob()
        bucket.blob.return_value = blob
        store.create_if_absent('k', b'body')
        kwargs = blob.upload_from_string.call_args.kwargs
        assert kwargs['timeout'] == 10
        assert kwargs['retry']._deadline == 30


class TestReplaceIfMatch:
    def test_uploads_with_the_observed_generation(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob(generation=9)
        bucket.blob.return_value = blob
        assert store.replace_if_match('k', b'body', '8') == '9'
        assert blob.upload_from_string.call_args.kwargs['if_generation_match'] == 8

    def test_lost_lock_returns_none(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob()
        blob.upload_from_string.side_effect = gcs_exceptions.PreconditionFailed('changed')
        bucket.blob.return_value = blob
        assert store.replace_if_match('k', b'body', '8') is None

    def test_a_missing_bucket_propagates(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob()
        blob.upload_from_string.side_effect = gcs_exceptions.NotFound('no such bucket')
        bucket.blob.return_value = blob
        with pytest.raises(gcs_exceptions.NotFound):
            store.replace_if_match('k', b'body', '8')


class TestDeleteIfMatch:
    def test_deletes_with_the_held_generation(self, store: GCSLockStore, bucket: MagicMock):
        blob = _blob()
        bucket.blob.return_value = blob
        assert store.delete_if_match('k', '8')
        blob.delete.assert_called_once_with(if_generation_match=8, timeout=ANY, retry=ANY)

    @pytest.mark.parametrize('error', [gcs_exceptions.PreconditionFailed('changed'), gcs_exceptions.NotFound('gone')])
    def test_lost_lock_returns_false(self, store: GCSLockStore, bucket: MagicMock, error: Exception):
        blob = _blob()
        blob.delete.side_effect = error
        bucket.blob.return_value = blob
        assert not store.delete_if_match('k', '8')


class TestStat:
    def test_returns_generation_server_time_and_body(self, store: GCSLockStore, bucket: MagicMock):
        bucket.get_blob.return_value = _blob(generation=5)
        pinned = _blob(generation=5)
        pinned.download_as_bytes.return_value = b'body'
        bucket.blob.return_value = pinned
        stored = store.stat('k')
        assert stored is not None
        assert (stored.version, stored.updated_at, stored.body) == ('5', datetime(2026, 11, 22, tzinfo=UTC).timestamp(), b'body')
        bucket.blob.assert_called_once_with('locks/run/k.lock', generation=5)

    def test_missing_object_returns_none(self, store: GCSLockStore, bucket: MagicMock):
        bucket.get_blob.return_value = None
        assert store.stat('k') is None

    def test_object_overwritten_between_stat_and_read_returns_none(self, store: GCSLockStore, bucket: MagicMock):
        bucket.get_blob.return_value = _blob(generation=5)
        pinned = _blob(generation=5)
        pinned.download_as_bytes.side_effect = gcs_exceptions.NotFound('overwritten')
        bucket.blob.return_value = pinned
        assert store.stat('k') is None


class TestClose:
    def test_close_closes_the_dedicated_client(self):
        client = MagicMock()
        GCSLockStore(client, 'my-bucket', 'locks').close()
        client.close.assert_called_once_with()


class TestRoundTripOnTheEmulator:
    """The emulator ignores the preconditions, but it pins what the mocks above assume: ``blob.generation`` is populated after an upload and changes on replace."""

    def test_create_stat_replace_delete(self, gcs_bucket: str):
        from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]

        store = GCSLockStore(storage.Client(), gcs_bucket, 'locks')
        created = store.create_if_absent('task_run', b'first')
        assert created is not None
        stored = store.stat('task_run')
        assert stored is not None
        assert (stored.version, stored.body) == (created, b'first')
        replaced = store.replace_if_match('task_run', b'second', created)
        assert replaced is not None and replaced != created
        assert store.delete_if_match('task_run', replaced)
        assert store.stat('task_run') is None
