import os
import luigi
import luigi.contrib.gcs


class GCSConfig(luigi.Config):
    gcs_credential_name = luigi.Parameter(
        default='GCS_CREDENTIAL', description='GCS credential environment variable.')

    def get_gcs_client(self) -> luigi.contrib.gcs.GCSClient:
        return luigi.contrib.gcs.GCSClient(
            oauth_credentials=os.environ.get(self.gcs_credential_name))
