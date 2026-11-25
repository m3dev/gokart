from __future__ import annotations

import json
import os
import warnings
from types import ModuleType
from typing import TYPE_CHECKING

import luigi

from gokart._optional_dependency import import_optional_dependency
from gokart.parameter import ByteSizeParameter, OptionalByteSizeParameter
from gokart.process_cache import cached_per_process

if TYPE_CHECKING:
    import luigi.contrib.gcs
    from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]
    from google.oauth2.service_account import Credentials


def import_storage() -> ModuleType:
    return _import_gcs_dependency('google.cloud.storage')


def _import_gcs_dependency(module_name: str) -> ModuleType:
    return import_optional_dependency(module_name, extra='gcs', feature='GCS')


class GCSConfig(luigi.Config):
    gcs_credential_name: luigi.StrParameter = luigi.StrParameter(default='GCS_CREDENTIAL', description='GCS credential environment variable.')
    parallel_upload_threshold: OptionalByteSizeParameter = OptionalByteSizeParameter(
        default=None,
        description='Upload files of at least this size (bytes, or e.g. 256MiB) in parallel chunks with an XML API multipart upload. Off when unset.',
    )
    parallel_download_threshold: OptionalByteSizeParameter = OptionalByteSizeParameter(
        default=None,
        description='Download objects of at least this size (bytes, or e.g. 256MiB) in parallel ranged requests. Off when unset.',
    )
    parallel_chunk_size: ByteSizeParameter = ByteSizeParameter(
        default=32 * 1024 * 1024,
        description='Size of each chunk of a parallel upload or download (bytes, or e.g. 32MiB). Upload chunks must be 5 MiB to 5 GiB.',
    )
    parallel_max_workers: luigi.IntParameter = luigi.IntParameter(default=8, description='Number of concurrent requests of a parallel upload or download.')
    _client = None
    _storage_client: tuple[int, storage.Client] | None = None

    def get_storage_client(self) -> storage.Client:
        # requests.Session inside the client is not fork-safe
        self._storage_client = cached_per_process(self._storage_client, self._create_storage_client)
        return self._storage_client[1]

    def create_storage_client(self) -> storage.Client:
        """Build a client that is not shared with anyone else, e.g. for a background thread."""
        return self._create_storage_client()

    def _create_storage_client(self) -> storage.Client:
        storage = import_storage()
        credentials = self._load_oauth_credentials()
        return storage.Client(project=getattr(credentials, 'project_id', None), credentials=credentials)

    def get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        # FutureWarning, because DeprecationWarning is hidden by default unless the caller is __main__.
        warnings.warn(
            'GCSConfig.get_gcs_client() is deprecated and will be removed in gokart 2.0.0. '
            'gokart no longer uses luigi.contrib.gcs; use GCSConfig.get_storage_client(), which returns a google.cloud.storage.Client, instead.',
            FutureWarning,
            stacklevel=2,
        )
        if self._client is None:  # use cache as like singleton object
            self._client = self._get_gcs_client()
        return self._client

    def _get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        _import_gcs_dependency('googleapiclient')
        import luigi.contrib.gcs

        return luigi.contrib.gcs.GCSClient(oauth_credentials=self._load_oauth_credentials())

    def _load_oauth_credentials(self) -> Credentials | None:
        service_account = _import_gcs_dependency('google.oauth2.service_account')

        json_str = os.environ.get(self.gcs_credential_name)
        if not json_str:
            return None

        if os.path.isfile(json_str):
            return service_account.Credentials.from_service_account_file(json_str)

        return service_account.Credentials.from_service_account_info(json.loads(json_str))
