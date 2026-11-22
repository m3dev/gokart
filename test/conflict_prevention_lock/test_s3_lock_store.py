from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from gokart.conflict_prevention_lock.object_storage_lock import ObjectStorageRunLock
from gokart.conflict_prevention_lock.s3_lock_store import S3LockStore, UnsupportedConditionalWritesError


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    with mock_aws():
        client = boto3.client('s3', region_name='us-east-1')
        client.create_bucket(Bucket='lock-bucket')
        yield client


@pytest.fixture
def store(client: Any) -> S3LockStore:
    return S3LockStore(client, 'lock-bucket', 'locks/run')


class TestObjectName:
    def test_object_is_named_prefix_key_lock(self, store: S3LockStore, client: Any):
        store.create_if_absent('task_run', b'x')
        assert client.get_object(Bucket='lock-bucket', Key='locks/run/task_run.lock')['Body'].read() == b'x'

    def test_empty_prefix_has_no_leading_slash(self, client: Any):
        S3LockStore(client, 'lock-bucket', '').create_if_absent('task_run', b'x')
        assert client.get_object(Bucket='lock-bucket', Key='task_run.lock')['Body'].read() == b'x'


class TestCreateIfAbsent:
    def test_returns_the_etag(self, store: S3LockStore):
        assert store.create_if_absent('k', b'body')

    def test_existing_object_fails_the_precondition(self, store: S3LockStore):
        assert store.create_if_absent('k', b'first')
        assert store.create_if_absent('k', b'second') is None
        stored = store.stat('k')
        assert stored is not None and stored.body == b'first'


class TestReplaceIfMatch:
    def test_replaces_when_the_etag_matches(self, store: S3LockStore):
        version = store.create_if_absent('k', b'first')
        assert version is not None
        new_version = store.replace_if_match('k', b'second', version)
        assert new_version is not None and new_version != version

    def test_stale_etag_fails_the_precondition(self, store: S3LockStore):
        version = store.create_if_absent('k', b'first')
        assert version is not None
        assert store.replace_if_match('k', b'second', version)
        assert store.replace_if_match('k', b'third', version) is None
        stored = store.stat('k')
        assert stored is not None and stored.body == b'second'

    def test_missing_object_returns_none(self, store: S3LockStore):
        assert store.replace_if_match('k', b'body', '"etag"') is None


class TestDeleteIfMatch:
    def test_deletes_when_the_etag_matches(self, store: S3LockStore):
        version = store.create_if_absent('k', b'body')
        assert version is not None
        assert store.delete_if_match('k', version)
        assert store.stat('k') is None

    def test_stale_etag_keeps_the_object(self, store: S3LockStore):
        assert store.create_if_absent('k', b'body')
        assert not store.delete_if_match('k', '"stale"')
        assert store.stat('k') is not None

    def test_missing_object_returns_false(self, store: S3LockStore):
        assert not store.delete_if_match('k', '"etag"')


class TestStat:
    def test_returns_etag_server_time_and_body(self, store: S3LockStore):
        version = store.create_if_absent('k', b'body')
        stored = store.stat('k')
        assert stored is not None
        assert (stored.version, stored.body) == (version, b'body')
        assert stored.updated_at > 0

    def test_missing_object_returns_none(self, store: S3LockStore):
        assert store.stat('k') is None


class TestUnsupportedStores:
    def test_store_rejecting_conditional_headers_is_reported_clearly(self, store: S3LockStore, client: Any):
        def reject(**_: Any) -> None:
            raise ClientError(
                {'Error': {'Code': 'NotImplemented', 'Message': 'A header you provided implies functionality that is not implemented'}}, 'PutObject'
            )

        client.put_object = reject
        with pytest.raises(UnsupportedConditionalWritesError, match='conditional'):
            store.create_if_absent('k', b'body')


class TestWithRunLock:
    def test_lock_lifecycle_on_s3(self, store: S3LockStore):
        first = ObjectStorageRunLock(store, 'task_run', ttl=60)
        second = ObjectStorageRunLock(store, 'task_run', ttl=60)
        assert first.try_acquire()
        assert not second.try_acquire()
        first.extend()
        first.release()
        assert second.try_acquire()
