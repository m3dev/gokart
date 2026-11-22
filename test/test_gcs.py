import os
import tempfile
from datetime import datetime

import luigi.format
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]

from gokart._gcs import GCSClient, GCSTarget


def _upload(bucket: str, key: str, data: bytes) -> None:
    storage.Client().bucket(bucket).blob(key).upload_from_string(data)


class TestGCSClient:
    def test_exists(self, gcs_bucket):
        _upload(gcs_bucket, 'dir/data.pkl', b'x')
        client = GCSClient()

        assert client.exists(f'gs://{gcs_bucket}/dir/data.pkl')
        assert not client.exists(f'gs://{gcs_bucket}/dir/missing.pkl')

    def test_put_uploads_local_file(self, gcs_bucket, tmp_path):
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        assert storage.Client().bucket(gcs_bucket).blob('data.bin').download_as_bytes() == b'hello'

    def test_put_sets_content_type_from_destination(self, gcs_bucket, tmp_path):
        local = tmp_path / 'luigi-tmp-123'
        local.write_bytes(b'a,b')

        GCSClient().put(str(local), f'gs://{gcs_bucket}/data.csv')

        blob = storage.Client().bucket(gcs_bucket).get_blob('data.csv')
        assert blob is not None
        assert blob.content_type == 'text/csv'

    def test_download_missing_object_leaves_no_temporary_file(self, gcs_bucket, tmp_path, monkeypatch):
        monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))

        with pytest.raises(NotFound):
            GCSClient().download(f'gs://{gcs_bucket}/missing.bin')

        assert list(tmp_path.iterdir()) == []

    def test_put_attaches_metadata(self, gcs_bucket, tmp_path):
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin', metadata={'owner': 'me'})

        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.metadata == {'owner': 'me'}

    def test_download_returns_file_removed_on_close(self, gcs_bucket):
        _upload(gcs_bucket, 'data.bin', b'hello')

        f = GCSClient().download(f'gs://{gcs_bucket}/data.bin')
        assert f.read() == b'hello'
        f.close()

        assert not os.path.exists(f.name)

    def test_download_empty_object(self, gcs_bucket):
        _upload(gcs_bucket, 'empty.bin', b'')

        with GCSClient().download(f'gs://{gcs_bucket}/empty.bin') as f:
            assert f.read() == b''

    def test_get_timestamp_returns_timezone_aware_datetime(self, gcs_bucket):
        _upload(gcs_bucket, 'data.bin', b'x')

        t = GCSClient().get_timestamp(f'gs://{gcs_bucket}/data.bin')

        assert isinstance(t, datetime)
        assert t.tzinfo is not None

    def test_get_timestamp_of_missing_object_raises_file_not_found(self, gcs_bucket):
        with pytest.raises(FileNotFoundError):
            GCSClient().get_timestamp(f'gs://{gcs_bucket}/missing.bin')

    def test_remove(self, gcs_bucket):
        _upload(gcs_bucket, 'data.bin', b'x')
        client = GCSClient()

        assert client.remove(f'gs://{gcs_bucket}/data.bin')
        assert not client.exists(f'gs://{gcs_bucket}/data.bin')

    def test_remove_missing_object_returns_false(self, gcs_bucket):
        assert not GCSClient().remove(f'gs://{gcs_bucket}/missing.bin')


class TestGCSTarget:
    def test_write_then_read(self, gcs_bucket):
        target = GCSTarget(f'gs://{gcs_bucket}/dir/data.bin', format=luigi.format.Nop)

        with target.open('w') as f:
            f.write(b'hello')

        assert target.exists()
        with target.open('r') as f:
            assert f.read() == b'hello'

    def test_nothing_is_uploaded_when_write_fails(self, gcs_bucket):
        target = GCSTarget(f'gs://{gcs_bucket}/data.bin', format=luigi.format.Nop)

        try:
            with target.open('w') as f:
                f.write(b'partial')
                raise RuntimeError
        except RuntimeError:
            pass

        assert not target.exists()

    def test_remove(self, gcs_bucket):
        _upload(gcs_bucket, 'data.bin', b'x')
        target = GCSTarget(f'gs://{gcs_bucket}/data.bin', format=luigi.format.Nop)

        target.remove()

        assert not target.exists()
