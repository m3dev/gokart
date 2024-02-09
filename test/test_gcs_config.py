import os
import unittest
from unittest.mock import MagicMock, patch

from gokart.gcs_config import GCSConfig


class TestGCSConfig(unittest.TestCase):
    def test_get_gcs_client_without_gcs_credential_name(self):
        mock = MagicMock()
        os.environ['env_name'] = ''
        with patch('luigi.contrib.gcs.GCSClient', mock):
            GCSConfig(gcs_credential_name='env_name')._get_gcs_client()
            self.assertEqual(dict(oauth_credentials=None), mock.call_args[1])

    def test_get_gcs_client_with_file_path(self):
        mock = MagicMock()
        file_path = 'test.json'
        os.environ['env_name'] = file_path
        with patch('luigi.contrib.gcs.GCSClient'):
            with patch('google.oauth2.service_account.Credentials.from_service_account_file', mock):
                with patch('os.path.isfile', return_value=True):
                    GCSConfig(gcs_credential_name='env_name')._get_gcs_client()
                    self.assertEqual(file_path, mock.call_args[0][0])

    def test_get_gcs_client_with_json(self):
        mock = MagicMock()
        json_str = '{"test": 1}'
        os.environ['env_name'] = json_str
        with patch('luigi.contrib.gcs.GCSClient'):
            with patch('google.oauth2.service_account.Credentials.from_service_account_info', mock):
                GCSConfig(gcs_credential_name='env_name')._get_gcs_client()
                self.assertEqual(dict(test=1), mock.call_args[0][0])
