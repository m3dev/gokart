import json
import os
import fcntl
import uritemplate

import luigi
import luigi.contrib.gcs
from http import client as http_client
from googleapiclient.errors import HttpError
from google.oauth2.service_account import Credentials
from googleapiclient.http import build_http
from googleapiclient.discovery import _retrieve_discovery_doc


class GCSConfig(luigi.Config):
    gcs_credential_name: str = luigi.Parameter(default='GCS_CREDENTIAL', description='GCS credential environment variable.')
    discover_cache_local_path: str = luigi.Parameter(default='DISCOVER_CACHE_LOCAL_PATH', description='The file path of discover api cache.')

    _DISCOVERY_URI = ("https://www.googleapis.com/discovery/v1/apis/" "{api}/{apiVersion}/rest")
    _V2_DISCOVERY_URI = ("https://{api}.googleapis.com/$discovery/rest?" "version={apiVersion}")
    _client = None

    def get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        if self._client is None:  # use cache as like singleton object
            self._client = self._get_gcs_client()
        return self._client

    def _get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        if not os.path.isfile(self.discover_cache_local_path):
            with open(self.discover_cache_local_path, "w") as f:
                try:
                    fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)

                    params = {"api": "storage", "apiVersion": "v1"}
                    discovery_http = build_http()
                    for discovery_url in (self._DISCOVERY_URI, self._V2_DISCOVERY_URI):
                        requested_url = uritemplate.expand(discovery_url, params)
                        try:
                            content = _retrieve_discovery_doc(requested_url, discovery_http, False)
                        except HttpError as e:
                            if e.resp.status == http_client.NOT_FOUND:
                                continue
                            else:
                                raise e
                        break
                    f.write(content)
                    fcntl.flock(f, fcntl.LOCK_UN)
                except IOError:
                    # try to read
                    pass

        with open(self.discover_cache_local_path, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            descriptor = f.read()
            fcntl.flock(f, fcntl.LOCK_UN)
            return luigi.contrib.gcs.GCSClient(oauth_credentials=self._load_oauth_credentials(), descriptor=descriptor)

    def _load_oauth_credentials(self) -> Credentials:
        json_str = os.environ.get(self.gcs_credential_name)
        if not json_str:
            return None

        if os.path.isfile(json_str):
            return Credentials.from_service_account_file(json_str)

        return Credentials.from_service_account_info(json.loads(json_str))
