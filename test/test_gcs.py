import os
import tempfile
from collections.abc import Callable, Iterator
from datetime import datetime
from unittest.mock import patch

import luigi
import luigi.format
import pytest
from google.api_core.exceptions import NotFound
from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]
from google.cloud.storage import transfer_manager  # pyright: ignore[reportMissingTypeStubs]
from google.cloud.storage.retry import DEFAULT_RETRY  # pyright: ignore[reportMissingTypeStubs]

from gokart._gcs import GCSClient, GCSTarget


def _upload(bucket: str, key: str, data: bytes) -> None:
    storage.Client().bucket(bucket).blob(key).upload_from_string(data)


@pytest.fixture
def gcs_config_section() -> Iterator[Callable[..., None]]:
    config = luigi.configuration.get_config()
    config.add_section('GCSConfig')

    def _set(**values: str) -> None:
        for name, value in values.items():
            config.set('GCSConfig', name, value)

    yield _set
    config.remove_section('GCSConfig')


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


class TestGCSClientParallelUpload:
    # the emulator does not implement the XML multipart upload API (501), so the upload is checked at the transfer_manager boundary

    def test_large_file_is_uploaded_in_chunks_with_labels(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='10', parallel_chunk_size='6MiB', parallel_max_workers='3')
        local = tmp_path / 'luigi-tmp-123'
        local.write_bytes(b'x' * 10)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/dir/data.csv', metadata={'owner': 'me'})

        upload.assert_called_once()
        (filename, blob), kwargs = upload.call_args
        assert filename == str(local)
        assert (blob.bucket.name, blob.name, blob.metadata) == (gcs_bucket, 'dir/data.csv', {'owner': 'me'})
        assert kwargs == dict(
            content_type='text/csv',
            chunk_size=6291456,
            max_workers=3,
            worker_type=transfer_manager.THREAD,
            retry=DEFAULT_RETRY,
        )

    def test_file_below_threshold_is_uploaded_in_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='10')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'x' * 9)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()
        assert storage.Client().bucket(gcs_bucket).blob('data.bin').download_as_bytes() == b'x' * 9

    def test_parallel_upload_is_off_by_default(self, gcs_bucket, tmp_path):
        local = tmp_path / 'data.bin'
        local.write_bytes(b'x' * 1024)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()

    def test_labels_that_cannot_be_http_headers_fall_back_to_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        # XML multipart upload sends labels as x-goog-meta-* headers, which cannot carry non-ASCII text
        gcs_config_section(parallel_upload_threshold='1')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin', metadata={'name': 'データ'})

        upload.assert_not_called()
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.metadata == {'name': 'データ'}

    def test_labels_whose_names_cannot_be_http_headers_fall_back_to_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='1')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin', metadata={'team:name': 'ml'})

        upload.assert_not_called()
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.metadata == {'team:name': 'ml'}

    def test_labels_with_upper_case_names_fall_back_to_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        # the XML API lowercases metadata keys
        gcs_config_section(parallel_upload_threshold='1')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin', metadata={'Owner': 'me'})

        upload.assert_not_called()
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.metadata == {'Owner': 'me'}

    def test_empty_file_is_uploaded_in_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        # a multipart upload with no parts cannot be completed
        gcs_config_section(parallel_upload_threshold='0')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'')

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()
        assert storage.Client().bucket(gcs_bucket).blob('data.bin').download_as_bytes() == b''

    def test_chunk_size_grows_to_stay_within_the_part_limit(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='1', parallel_chunk_size='5MiB')
        local = tmp_path / 'data.bin'
        size = 5 * 1024 * 1024 * 10_000 + 1
        with open(local, 'wb') as f:
            f.truncate(size)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        chunk_size = upload.call_args.kwargs['chunk_size']
        assert -(-size // chunk_size) <= 10_000

    def test_non_positive_max_workers_is_rejected_before_uploading(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='1', parallel_max_workers='0')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            with pytest.raises(ValueError, match='parallel_max_workers'):
                GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()

    def test_chunk_size_below_the_multipart_minimum_is_rejected_before_uploading(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='1', parallel_chunk_size='1024')
        local = tmp_path / 'data.bin'
        local.write_bytes(b'hello')

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            with pytest.raises(ValueError, match='parallel_chunk_size'):
                GCSClient().put(str(local), f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()


class TestGCSClientParallelDownload:
    def test_large_object_is_downloaded_in_chunks(self, gcs_bucket, gcs_config_section):
        data = bytes(range(256)) * 4
        _upload(gcs_bucket, 'data.bin', data)
        gcs_config_section(parallel_download_threshold=str(len(data)), parallel_chunk_size='100', parallel_max_workers='3')

        with patch.object(transfer_manager, 'download_chunks_concurrently', wraps=transfer_manager.download_chunks_concurrently) as download:
            with GCSClient().download(f'gs://{gcs_bucket}/data.bin') as f:
                assert f.read() == data

        download.assert_called_once()
        assert download.call_args.kwargs == dict(chunk_size=100, max_workers=3, worker_type=transfer_manager.THREAD)

    def test_object_below_threshold_is_downloaded_in_one_stream(self, gcs_bucket, gcs_config_section):
        _upload(gcs_bucket, 'data.bin', b'x' * 9)
        gcs_config_section(parallel_download_threshold='10')

        with patch.object(transfer_manager, 'download_chunks_concurrently') as download:
            with GCSClient().download(f'gs://{gcs_bucket}/data.bin') as f:
                assert f.read() == b'x' * 9

        download.assert_not_called()

    def test_gzip_encoded_object_is_downloaded_in_one_stream(self, gcs_bucket, gcs_config_section):
        # GCS ignores Range while decompressing such an object, so ranged chunks would each get the whole object
        blob = storage.Client().bucket(gcs_bucket).blob('data.bin')
        blob.content_encoding = 'gzip'
        blob.upload_from_string(b'x' * 10)
        gcs_config_section(parallel_download_threshold='1')

        with patch.object(transfer_manager, 'download_chunks_concurrently') as download:
            GCSClient().download(f'gs://{gcs_bucket}/data.bin').close()

        download.assert_not_called()

    def test_non_positive_chunk_size_is_rejected(self, gcs_bucket, gcs_config_section):
        _upload(gcs_bucket, 'data.bin', b'x' * 10)
        gcs_config_section(parallel_download_threshold='1', parallel_chunk_size='0')

        with pytest.raises(ValueError, match='parallel_chunk_size'):
            GCSClient().download(f'gs://{gcs_bucket}/data.bin')

    def test_missing_object_raises_not_found_and_leaves_no_temporary_file(self, gcs_bucket, gcs_config_section, tmp_path, monkeypatch):
        gcs_config_section(parallel_download_threshold='1')
        monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))

        with pytest.raises(NotFound):
            GCSClient().download(f'gs://{gcs_bucket}/missing.bin')

        assert list(tmp_path.iterdir()) == []


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
