import os
import unittest
from unittest.mock import MagicMock, patch

import pytest

from gokart.gcs_config import GCSConfig


class TestGCSConfig(unittest.TestCase):
    def test_get_gcs_client_without_gcs_credential_name(self):
        mock = MagicMock()
        with patch.dict(os.environ, {'env_name': ''}), patch('luigi.contrib.gcs.GCSClient', mock):
            GCSConfig(gcs_credential_name='env_name')._get_gcs_client()
            self.assertEqual(dict(oauth_credentials=None), mock.call_args[1])

    def test_get_gcs_client_with_file_path(self):
        mock = MagicMock()
        file_path = 'test.json'
        with patch.dict(os.environ, {'env_name': file_path}), patch('luigi.contrib.gcs.GCSClient'):
            with patch('google.oauth2.service_account.Credentials.from_service_account_file', mock):
                with patch('os.path.isfile', return_value=True):
                    GCSConfig(gcs_credential_name='env_name')._get_gcs_client()
                    self.assertEqual(file_path, mock.call_args[0][0])

    def test_get_gcs_client_with_json(self):
        mock = MagicMock()
        json_str = '{"test": 1}'
        with patch.dict(os.environ, {'env_name': json_str}), patch('luigi.contrib.gcs.GCSClient'):
            with patch('google.oauth2.service_account.Credentials.from_service_account_info', mock):
                GCSConfig(gcs_credential_name='env_name')._get_gcs_client()
                self.assertEqual(dict(test=1), mock.call_args[0][0])


class TestGetStorageClient:
    def test_returns_storage_client_for_emulator(self, gcs_bucket):
        client = GCSConfig().get_storage_client()

        assert client.bucket(gcs_bucket).exists()

    def test_caches_client_within_process(self, gcs_bucket):
        config = GCSConfig()

        assert config.get_storage_client() is config.get_storage_client()

    def test_recreates_client_after_fork(self, gcs_bucket):
        config = GCSConfig()
        client = config.get_storage_client()

        # requests.Session is not fork-safe, so a forked worker must not reuse the parent's client
        with patch('os.getpid', return_value=os.getpid() + 1):
            assert config.get_storage_client() is not client

    def test_passes_credentials_from_env(self, monkeypatch):
        credentials = MagicMock()
        credentials.project_id = 'my-project'
        monkeypatch.setenv('env_name', '{"test": 1}')

        with (
            patch('google.oauth2.service_account.Credentials.from_service_account_info', return_value=credentials),
            patch('google.cloud.storage.Client') as client_cls,
        ):
            GCSConfig(gcs_credential_name='env_name').get_storage_client()

        client_cls.assert_called_once_with(project='my-project', credentials=credentials)

    def test_uses_default_credentials_when_env_is_empty(self, monkeypatch):
        monkeypatch.setenv('env_name', '')

        with patch('google.cloud.storage.Client') as client_cls:
            GCSConfig(gcs_credential_name='env_name').get_storage_client()

        client_cls.assert_called_once_with(project=None, credentials=None)


class TestGetGCSClientDeprecation:
    def test_warns_and_points_to_get_storage_client(self):
        with patch('luigi.contrib.gcs.GCSClient'), pytest.warns(DeprecationWarning, match='get_storage_client'):
            GCSConfig().get_gcs_client()
