import os
from unittest.mock import MagicMock, patch

from gokart.gcs_config import GCSConfig


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

    def test_loads_credentials_from_file_path(self, monkeypatch):
        monkeypatch.setenv('env_name', 'test.json')

        with (
            patch('os.path.isfile', return_value=True),
            patch('google.oauth2.service_account.Credentials.from_service_account_file') as from_file,
            patch('google.cloud.storage.Client') as client_cls,
        ):
            GCSConfig(gcs_credential_name='env_name').get_storage_client()

        from_file.assert_called_once_with('test.json')
        client_cls.assert_called_once_with(project=from_file.return_value.project_id, credentials=from_file.return_value)

    def test_uses_default_credentials_when_env_is_empty(self, monkeypatch):
        monkeypatch.setenv('env_name', '')

        with patch('google.cloud.storage.Client') as client_cls:
            GCSConfig(gcs_credential_name='env_name').get_storage_client()

        client_cls.assert_called_once_with(project=None, credentials=None)


class TestGetGCSClientRemoved:
    def test_get_gcs_client_is_removed(self):
        # gokart 2.0.0 removed the luigi.contrib.gcs client built on google-api-python-client; get_storage_client() replaces it.
        assert not hasattr(GCSConfig, 'get_gcs_client')
