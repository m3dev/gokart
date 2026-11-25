from __future__ import annotations

import os
import tempfile
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

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

    def test_make_archive_overwrites_existing_archive(self, tmp_path):
        file_path = str(tmp_path / 'archive.zip')
        _make_archive_with(tmp_path / 'old', file_path, 'old')

        _make_archive_with(tmp_path / 'new', file_path, 'new')

        assert _unpack_text(tmp_path, file_path) == 'new'

    def test_make_archive_keeps_previous_archive_readable_while_writing(self, tmp_path):
        # Readers without a target lock open the archive at any time, so they must never see a partially written one.
        file_path = str(tmp_path / 'archive.zip')
        _make_archive_with(tmp_path / 'old', file_path, 'old')
        texts_seen_while_writing = []
        original_write = zipfile.ZipFile.write

        def write_and_read_archive(self, *args, **kwargs):
            original_write(self, *args, **kwargs)
            texts_seen_while_writing.append(_unpack_text(tmp_path, file_path))

        with patch.object(zipfile.ZipFile, 'write', write_and_read_archive):
            _make_archive_with(tmp_path / 'new', file_path, 'new')

        assert texts_seen_while_writing == ['old']
        assert _unpack_text(tmp_path, file_path) == 'new'

    def test_make_archive_leaves_no_file_behind_when_writing_fails(self, tmp_path):
        temporary_directory = tmp_path / 'tmp'
        temporary_directory.mkdir()
        (temporary_directory / 'data.txt').write_text('test')
        zip_client = LocalZipClient(file_path=str(tmp_path / 'archive.zip'), temporary_directory=str(temporary_directory))

        with patch.object(zipfile.ZipFile, 'write', side_effect=OSError('disk full')), pytest.raises(OSError, match='disk full'):
            zip_client.make_archive()

        assert os.listdir(tmp_path) == ['tmp']

    def test_make_archive_writes_through_symlink(self, tmp_path):
        real_path = tmp_path / 'real' / 'archive.zip'
        real_path.parent.mkdir()
        _make_archive_with(tmp_path / 'old', str(real_path), 'old')
        link_path = tmp_path / 'link.zip'
        link_path.symlink_to(real_path)

        _make_archive_with(tmp_path / 'new', str(link_path), 'new')

        assert link_path.is_symlink()
        assert _unpack_text(tmp_path, str(real_path)) == 'new'

    def test_make_archive_accepts_file_name_at_file_system_limit(self, tmp_path):
        name_max = os.pathconf(tmp_path, 'PC_NAME_MAX')
        file_path = str(tmp_path / ('a' * (name_max - len('.zip')) + '.zip'))

        _make_archive_with(tmp_path / 'new', file_path, 'new')

        assert _unpack_text(tmp_path, file_path) == 'new'


def _make_archive_with(temporary_directory: Path, file_path: str, text: str) -> None:
    temporary_directory.mkdir()
    (temporary_directory / 'data.txt').write_text(text)
    LocalZipClient(file_path=file_path, temporary_directory=str(temporary_directory)).make_archive()


def _unpack_text(tmp_path: Path, file_path: str) -> str:
    extract_directory = tempfile.mkdtemp(dir=tmp_path)
    LocalZipClient(file_path=file_path, temporary_directory=extract_directory).unpack_archive()
    return (Path(extract_directory) / 'data.txt').read_text()
