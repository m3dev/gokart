import json
import os

import luigi
import luigi.contrib.gcs
from google.oauth2.service_account import Credentials


class GCSConfig(luigi.Config):
    gcs_credential_name: str = luigi.Parameter(default='GCS_CREDENTIAL', description='GCS credential environment variable.')
    _client = None

    def get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        if self._client is None:  # use cache as like singleton object
            self._client = self._get_gcs_client()
        return self._client

    def _get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        return luigi.contrib.gcs.GCSClient(oauth_credentials=self._load_oauth_credentials())

    def _load_oauth_credentials(self) -> Credentials:
        json_str = os.environ.get(self.gcs_credential_name)
        if not json_str:
            return None

        if os.path.isfile(json_str):
            return Credentials.from_service_account_file(json_str)

        return Credentials.from_service_account_info(json.loads(json_str))
