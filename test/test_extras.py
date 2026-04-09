from __future__ import annotations

import sys
from collections.abc import Callable
from typing import Any
from unittest.mock import patch

import luigi.format
import pytest


def _make_import_raiser(blocked_modules: set[str]) -> Callable[..., Any]:
    """Return a side_effect for builtins.__import__ that raises ImportError for blocked modules."""
    original_import = __builtins__.__import__ if hasattr(__builtins__, '__import__') else __import__

    def _import_raiser(name, *args, **kwargs):
        for blocked in blocked_modules:
            if name == blocked or name.startswith(blocked + '.'):
                raise ImportError(f'No module named {name!r}')
        return original_import(name, *args, **kwargs)

    return _import_raiser


class TestS3ExtrasNotInstalled:
    def test_s3_config_raises_import_error_without_boto3(self):
        from gokart.s3_config import S3Config

        config = S3Config()
        config._client = None

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.s3'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
                config._get_s3_client()

    def test_object_storage_get_target_raises_for_s3_without_extras(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.s3'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
                ObjectStorage.get_object_storage_target('s3://bucket/key', format=luigi.format.Nop)

    def test_object_storage_exists_raises_for_s3_without_extras(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.s3'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
                ObjectStorage.exists('s3://bucket/key')

    def test_object_storage_get_zip_client_raises_for_s3_without_extras(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.s3'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
                ObjectStorage.get_zip_client('s3://bucket/key.zip', '/tmp/test')

    def test_s3_zip_client_raises_without_extras(self):
        from gokart.s3_zip_client import S3ZipClient

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.s3'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
                S3ZipClient(file_path='s3://bucket/key.zip', temporary_directory='/tmp/test')

    def test_object_storage_is_buffered_reader_returns_true_without_s3(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.s3'})):
            # s3 module is already cached in sys.modules, so we also need to remove it
            saved = sys.modules.pop('luigi.contrib.s3', None)
            try:
                assert ObjectStorage.is_buffered_reader(object()) is True
            finally:
                if saved is not None:
                    sys.modules['luigi.contrib.s3'] = saved


class TestGCSExtrasNotInstalled:
    def test_gcs_config_raises_import_error_without_gcs_lib(self):
        from gokart.gcs_config import GCSConfig

        config = GCSConfig()
        config._client = None

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.gcs'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                config._get_gcs_client()

    def test_gcs_config_load_credentials_raises_without_google_auth(self):
        from gokart.gcs_config import GCSConfig

        config = GCSConfig()

        with patch('builtins.__import__', side_effect=_make_import_raiser({'google.oauth2'})):
            # Need to remove cached module
            saved = sys.modules.pop('google.oauth2.service_account', None)
            saved_parent = sys.modules.pop('google.oauth2', None)
            try:
                with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                    config._load_oauth_credentials()
            finally:
                if saved is not None:
                    sys.modules['google.oauth2.service_account'] = saved
                if saved_parent is not None:
                    sys.modules['google.oauth2'] = saved_parent

    def test_gcs_obj_metadata_client_makepatch_raises_without_googleapiclient(self):
        from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient

        with patch('builtins.__import__', side_effect=_make_import_raiser({'googleapiclient'})):
            saved = sys.modules.pop('googleapiclient.model', None)
            saved_parent = sys.modules.pop('googleapiclient', None)
            try:
                with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                    GCSObjectMetadataClient._makepatch({}, {})
            finally:
                if saved is not None:
                    sys.modules['googleapiclient.model'] = saved
                if saved_parent is not None:
                    sys.modules['googleapiclient'] = saved_parent

    def test_object_storage_exists_raises_for_gcs_without_extras(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.gcs'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                ObjectStorage.exists('gs://bucket/key')

    def test_object_storage_get_zip_client_raises_for_gcs_without_extras(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.gcs'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                ObjectStorage.get_zip_client('gs://bucket/key.zip', '/tmp/test')

    def test_gcs_zip_client_raises_without_extras(self):
        from gokart.gcs_zip_client import GCSZipClient

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.gcs'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                GCSZipClient(file_path='gs://bucket/key.zip', temporary_directory='/tmp/test')

    def test_object_storage_get_target_raises_for_gcs_without_extras(self):
        from gokart.object_storage import ObjectStorage

        with patch('builtins.__import__', side_effect=_make_import_raiser({'luigi.contrib.gcs'})):
            with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                ObjectStorage.get_object_storage_target('gs://bucket/key', format=luigi.format.Nop)


class TestExtrasInstalled:
    """Verify that imports succeed when extras are installed (current test environment)."""

    def test_s3_config_can_be_imported(self):
        from gokart.s3_config import S3Config

        assert S3Config is not None

    def test_gcs_config_can_be_imported(self):
        from gokart.gcs_config import GCSConfig

        assert GCSConfig is not None

    def test_object_storage_can_be_imported(self):
        from gokart.object_storage import ObjectStorage

        assert ObjectStorage is not None

    def test_gcs_obj_metadata_client_can_be_imported(self):
        from gokart.gcs_obj_metadata_client import GCSObjectMetadataClient

        assert GCSObjectMetadataClient is not None

    def test_luigi_contrib_s3_is_importable(self):
        import luigi.contrib.s3

        assert luigi.contrib.s3 is not None

    def test_luigi_contrib_gcs_is_importable(self):
        import luigi.contrib.gcs

        assert luigi.contrib.gcs is not None
