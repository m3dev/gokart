import unittest
import os
from unittest.mock import patch, MagicMock, mock_open

from gokart.gcs_config import GCSConfig


class TestGCSConfig(unittest.TestCase):
    def test_get_gcs_client_without_gcs_credential_name(self):
        mock = MagicMock()
        discover_path = 'discover_cache.json'
        os.environ['env_name'] = ''
        os.environ['discover_path'] = discover_path
        with open(f'{discover_path}', 'w') as f:
            f.write('{}')
        with patch('luigi.contrib.gcs.GCSClient', mock):
            with patch('fcntl.flock'):
                GCSConfig(gcs_credential_name='env_name', discover_cache_local_path=discover_path)._get_gcs_client()
                self.assertEqual(dict(oauth_credentials=None, descriptor='{}'), mock.call_args[1])

    def test_get_gcs_client_with_file_path(self):
        mock = MagicMock()
        file_path = 'test.json'
        discover_path = 'discover_cache.json'
        os.environ['env_name'] = file_path
        os.environ['discover_path'] = discover_path
        with open(f'{discover_path}', 'w') as f:
            f.write('{}')
        with patch('luigi.contrib.gcs.GCSClient'):
            with patch('google.oauth2.service_account.Credentials.from_service_account_file', mock):
                with patch('os.path.isfile', return_value=True):
                    GCSConfig(gcs_credential_name='env_name', discover_cache_local_path=discover_path)._get_gcs_client()
                    self.assertEqual(file_path, mock.call_args[0][0])

    def test_get_gcs_client_with_json(self):
        mock = MagicMock()
        json_str = '{"test": 1}'
        discover_path = 'discover_cache.json'
        os.environ['env_name'] = json_str
        os.environ['discover_path'] = discover_path
        with open(f'{discover_path}', 'w') as f:
            f.write('{}')
        with patch('luigi.contrib.gcs.GCSClient'):
            with patch('google.oauth2.service_account.Credentials.from_service_account_info', mock):
                GCSConfig(gcs_credential_name='env_name', discover_cache_local_path=discover_path)._get_gcs_client()
                self.assertEqual(dict(test=1), mock.call_args[0][0])
