from __future__ import annotations

import json
import os
from typing import TYPE_CHECKING, cast

import luigi

if TYPE_CHECKING:
    import luigi.contrib.gcs
    from google.oauth2.service_account import Credentials


class GCSConfig(luigi.Config):
    gcs_credential_name: luigi.StrParameter = luigi.StrParameter(default='GCS_CREDENTIAL', description='GCS credential environment variable.')
    _client = None

    def get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        if self._client is None:  # use cache as like singleton object
            self._client = self._get_gcs_client()
        return self._client

    def _get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        try:
            import luigi.contrib.gcs
        except ImportError:
            raise ImportError('GCS support requires additional dependencies. Install them with: pip install gokart[gcs]') from None
        return luigi.contrib.gcs.GCSClient(oauth_credentials=self._load_oauth_credentials())

    def _load_oauth_credentials(self) -> Credentials | None:
        try:
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise ImportError('GCS support requires additional dependencies. Install them with: pip install gokart[gcs]') from None

        json_str = os.environ.get(self.gcs_credential_name)
        if not json_str:
            return None

        if os.path.isfile(json_str):
            return cast(Credentials, Credentials.from_service_account_file(json_str))

        return cast(Credentials, Credentials.from_service_account_info(json.loads(json_str)))
