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

from gokart._gcs import GCSClient, GCSTarget, _multipart_chunk_size


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


MiB = 1024 * 1024


def _write_file(path, size: int) -> str:
    with open(path, 'wb') as f:
        f.truncate(size)
    return str(path)


class TestGCSClientParallelSettings:
    @pytest.mark.parametrize(
        ('values', 'match'),
        [
            ({'parallel_upload_threshold': '1', 'parallel_chunk_size': str(5 * MiB - 1)}, 'parallel_chunk_size'),
            ({'parallel_download_threshold': '1', 'parallel_chunk_size': str(MiB - 1)}, 'parallel_chunk_size'),
            ({'parallel_upload_threshold': '1', 'parallel_max_workers': '0'}, 'parallel_max_workers'),
            ({'parallel_download_threshold': '1', 'parallel_max_workers': '0'}, 'parallel_max_workers'),
            ({'parallel_upload_threshold': '1', 'parallel_chunk_size': str(5 * 1024 * MiB + 1)}, 'parallel_chunk_size'),
        ],
    )
    def test_invalid_settings_are_rejected_when_the_client_is_created(self, gcs_config_section, values, match):
        gcs_config_section(**values)

        with pytest.raises(ValueError, match=match):
            GCSClient()

    @pytest.mark.parametrize(
        'values',
        [
            {'parallel_upload_threshold': '1', 'parallel_chunk_size': '5MiB'},
            {'parallel_upload_threshold': '1', 'parallel_chunk_size': '5GiB'},
            {'parallel_download_threshold': '1', 'parallel_chunk_size': '1MiB'},
            # the minimums only matter for the direction that is turned on
            {'parallel_chunk_size': '1', 'parallel_max_workers': '0'},
        ],
    )
    def test_valid_settings_are_accepted(self, gcs_config_section, values):
        gcs_config_section(**values)

        GCSClient()

    @pytest.mark.parametrize(
        ('size', 'expected'),
        [
            (5 * MiB * 10_000, 5 * MiB),
            (5 * MiB * 10_000 + 1, 5 * MiB + 1),
        ],
    )
    def test_multipart_chunk_size_stays_within_the_part_limit(self, size, expected):
        chunk_size = _multipart_chunk_size(size, 5 * MiB)

        assert chunk_size == expected
        assert -(-size // chunk_size) <= 10_000


class TestGCSClientParallelUpload:
    # the emulator does not implement the XML multipart upload API (501), so the upload is checked at the transfer_manager boundary

    def test_large_file_is_uploaded_in_chunks_with_labels(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='6MiB', parallel_chunk_size='5MiB')
        local = _write_file(tmp_path / 'luigi-tmp-123', 6 * MiB)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(local, f'gs://{gcs_bucket}/dir/data.csv', metadata={'owner': 'me'})

        upload.assert_called_once()
        (filename, blob), kwargs = upload.call_args
        assert filename == local
        assert (blob.bucket.name, blob.name, blob.metadata) == (gcs_bucket, 'dir/data.csv', {'owner': 'me'})
        assert kwargs['content_type'] == 'text/csv'
        assert kwargs['worker_type'] == transfer_manager.THREAD

    def test_file_below_threshold_is_uploaded_in_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        gcs_config_section(parallel_upload_threshold='6MiB', parallel_chunk_size='5MiB')
        local = _write_file(tmp_path / 'data.bin', 6 * MiB - 1)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(local, f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.size == 6 * MiB - 1

    def test_file_not_larger_than_one_chunk_is_uploaded_in_one_stream(self, gcs_bucket, gcs_config_section, tmp_path):
        # a single-part multipart upload costs two more requests, gains nothing and drops the MD5
        gcs_config_section(parallel_upload_threshold='1', parallel_chunk_size='5MiB')
        local = _write_file(tmp_path / 'data.bin', 5 * MiB)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(local, f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.size == 5 * MiB

    def test_parallel_upload_is_off_by_default(self, gcs_bucket, tmp_path):
        local = _write_file(tmp_path / 'data.bin', 64 * MiB)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(local, f'gs://{gcs_bucket}/data.bin')

        upload.assert_not_called()

    @pytest.mark.parametrize(
        'metadata',
        [
            # x-goog-meta-* headers cannot carry non-ASCII text
            {'name': 'データ'},
            # not an HTTP header name
            {'team:name': 'ml'},
            # the XML API lowercases metadata keys
            {'Owner': 'me'},
            # HTTP strips the value, so it may not be stored as given
            {'owner': ''},
        ],
    )
    def test_labels_that_cannot_be_http_headers_fall_back_to_one_stream(self, gcs_bucket, gcs_config_section, tmp_path, metadata):
        gcs_config_section(parallel_upload_threshold='1', parallel_chunk_size='5MiB')
        local = _write_file(tmp_path / 'data.bin', 5 * MiB + 1)

        with patch.object(transfer_manager, 'upload_chunks_concurrently') as upload:
            GCSClient().put(local, f'gs://{gcs_bucket}/data.bin', metadata=metadata)

        upload.assert_not_called()
        blob = storage.Client().bucket(gcs_bucket).get_blob('data.bin')
        assert blob is not None
        assert blob.metadata == metadata


class TestGCSClientParallelDownload:
    def test_large_object_is_downloaded_in_chunks(self, gcs_bucket, gcs_config_section):
        data = os.urandom(3 * MiB)
        _upload(gcs_bucket, 'data.bin', data)
        gcs_config_section(parallel_download_threshold=str(len(data)), parallel_chunk_size='1MiB')

        with (
            patch.object(transfer_manager, 'download_chunks_concurrently', wraps=transfer_manager.download_chunks_concurrently) as download,
            GCSClient().download(f'gs://{gcs_bucket}/data.bin') as f,
        ):
            assert f.read() == data

        download.assert_called_once()
        assert download.call_args.kwargs['worker_type'] == transfer_manager.THREAD

    def test_object_below_threshold_is_downloaded_in_one_stream(self, gcs_bucket, gcs_config_section):
        data = os.urandom(3 * MiB)
        _upload(gcs_bucket, 'data.bin', data)
        gcs_config_section(parallel_download_threshold=str(len(data) + 1), parallel_chunk_size='1MiB')

        with patch.object(transfer_manager, 'download_chunks_concurrently') as download, GCSClient().download(f'gs://{gcs_bucket}/data.bin') as f:
            assert f.read() == data

        download.assert_not_called()

    def test_object_not_larger_than_one_chunk_is_downloaded_in_one_stream(self, gcs_bucket, gcs_config_section):
        data = os.urandom(MiB)
        _upload(gcs_bucket, 'data.bin', data)
        gcs_config_section(parallel_download_threshold='1', parallel_chunk_size='1MiB')

        with patch.object(transfer_manager, 'download_chunks_concurrently') as download, GCSClient().download(f'gs://{gcs_bucket}/data.bin') as f:
            assert f.read() == data

        download.assert_not_called()

    def test_gzip_encoded_object_is_downloaded_in_one_stream(self, gcs_bucket, gcs_config_section):
        # GCS ignores Range while decompressing such an object, so ranged chunks would each get the whole object.
        # The emulator does not transcode, so this only checks that the parallel path is skipped.
        blob = storage.Client().bucket(gcs_bucket).blob('data.bin')
        blob.content_encoding = 'gzip'
        blob.upload_from_string(os.urandom(2 * MiB))
        gcs_config_section(parallel_download_threshold='1', parallel_chunk_size='1MiB')

        with patch.object(transfer_manager, 'download_chunks_concurrently') as download:
            GCSClient().download(f'gs://{gcs_bucket}/data.bin').close()

        download.assert_not_called()

    def test_failed_parallel_download_leaves_no_temporary_file(self, gcs_bucket, gcs_config_section, tmp_path, monkeypatch):
        _upload(gcs_bucket, 'data.bin', os.urandom(2 * MiB))
        gcs_config_section(parallel_download_threshold='1', parallel_chunk_size='1MiB')
        monkeypatch.setattr(tempfile, 'tempdir', str(tmp_path))

        def fail_halfway(blob, filename, **kwargs):
            with open(filename, 'wb') as f:
                f.write(b'partial')
            raise RuntimeError('checksum mismatch')

        with (
            patch.object(transfer_manager, 'download_chunks_concurrently', side_effect=fail_halfway),
            pytest.raises(RuntimeError, match='checksum mismatch'),
        ):
            GCSClient().download(f'gs://{gcs_bucket}/data.bin')

        assert list(tmp_path.iterdir()) == []

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
