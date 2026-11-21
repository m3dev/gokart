from __future__ import annotations

import json
import os
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

import luigi

if TYPE_CHECKING:
    import luigi.contrib.gcs
    from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]
    from google.oauth2.service_account import Credentials


_MISSING_GCS_EXTRA_MESSAGE = 'GCS support requires additional dependencies. Install them with: pip install gokart[gcs]'


def import_storage() -> ModuleType:
    try:
        from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]
    except ImportError:
        raise ImportError(_MISSING_GCS_EXTRA_MESSAGE) from None
    return storage


class GCSConfig(luigi.Config):
    gcs_credential_name: luigi.StrParameter = luigi.StrParameter(default='GCS_CREDENTIAL', description='GCS credential environment variable.')
    _client = None
    _storage_client: tuple[int, storage.Client] | None = None

    def get_storage_client(self) -> storage.Client:
        # requests.Session inside the client is not fork-safe, so each process (e.g. worker_processes > 1) builds its own.
        # No lock on purpose: threads racing on the first call may each build a client, which is harmless since the last write wins.
        pid = os.getpid()
        if self._storage_client is None or self._storage_client[0] != pid:
            self._storage_client = (pid, self._create_storage_client())
        return self._storage_client[1]

    def _create_storage_client(self) -> storage.Client:
        storage = import_storage()
        credentials = self._load_oauth_credentials()
        return storage.Client(project=getattr(credentials, 'project_id', None), credentials=credentials)

    def get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        if self._client is None:  # use cache as like singleton object
            self._client = self._get_gcs_client()
        return self._client

    def _get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        try:
            import googleapiclient  # noqa: F401  # pyright: ignore[reportUnusedImport]
        except ImportError:
            raise ImportError(_MISSING_GCS_EXTRA_MESSAGE) from None
        import luigi.contrib.gcs

        return luigi.contrib.gcs.GCSClient(oauth_credentials=self._load_oauth_credentials())

    def _load_oauth_credentials(self) -> Credentials | None:
        try:
            from google.oauth2.service_account import Credentials
        except ImportError:
            raise ImportError(_MISSING_GCS_EXTRA_MESSAGE) from None

        json_str = os.environ.get(self.gcs_credential_name)
        if not json_str:
            return None

        if os.path.isfile(json_str):
            return cast(Any, Credentials).from_service_account_file(json_str)

        return cast(Any, Credentials).from_service_account_info(json.loads(json_str))
