from __future__ import annotations

import os

from gokart.zip_client import LocalZipClient


class TestLocalZipClient:
    def test_remove_deletes_archive(self, tmp_path):
        temporary_directory = tmp_path / 'tmp'
        temporary_directory.mkdir()
        (temporary_directory / 'data.txt').write_text('test')
        file_path = str(tmp_path / 'archive.zip')
        zip_client = LocalZipClient(file_path=file_path, temporary_directory=str(temporary_directory))
        zip_client.make_archive()
        assert os.path.exists(file_path)

        zip_client.remove()

        assert not os.path.exists(file_path)

    def test_remove_does_not_raise_when_archive_does_not_exist(self, tmp_path):
        file_path = str(tmp_path / 'archive.zip')
        zip_client = LocalZipClient(file_path=file_path, temporary_directory=str(tmp_path / 'tmp'))

        zip_client.remove()

        assert not os.path.exists(file_path)

    def test_make_archive_accepts_and_ignores_metadata(self, tmp_path):
        # labels only exist on GCS, but ModelTarget passes them to whichever zip client it gets
        temporary_directory = tmp_path / 'tmp'
        temporary_directory.mkdir()
        file_path = str(tmp_path / 'archive.zip')

        LocalZipClient(file_path=file_path, temporary_directory=str(temporary_directory)).make_archive(metadata={'owner': 'me'})

        assert os.path.exists(file_path)
