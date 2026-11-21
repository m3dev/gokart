import os

from gokart.gcs_zip_client import GCSZipClient


class TestGCSZipClient:
    def test_make_and_unpack_archive(self, gcs_bucket, tmp_path):
        file_path = f'gs://{gcs_bucket}/model.zip'
        in_directory = tmp_path / 'in'
        in_directory.mkdir()
        (in_directory / 'weights.txt').write_text('w')

        in_client = GCSZipClient(file_path=file_path, temporary_directory=str(in_directory))
        in_client.make_archive()
        assert in_client.exists()

        out_directory = tmp_path / 'out'
        GCSZipClient(file_path=file_path, temporary_directory=str(out_directory)).unpack_archive()

        assert (out_directory / 'weights.txt').read_text() == 'w'

    def test_remove(self, gcs_bucket, tmp_path):
        in_directory = tmp_path / 'in'
        os.makedirs(in_directory)
        client = GCSZipClient(file_path=f'gs://{gcs_bucket}/model.zip', temporary_directory=str(in_directory))
        client.make_archive()

        client.remove()

        assert not client.exists()

    def test_remove_missing_archive_does_not_raise(self, gcs_bucket, tmp_path):
        GCSZipClient(file_path=f'gs://{gcs_bucket}/missing.zip', temporary_directory=str(tmp_path)).remove()
