from datetime import datetime

import luigi
import luigi.contrib.gcs
import luigi.contrib.s3
from luigi.format import Format

from gokart.gcs_config import GCSConfig
from gokart.gcs_zip_client import GCSZipClient
from gokart.s3_config import S3Config
from gokart.s3_zip_client import S3ZipClient
from gokart.zip_client import ZipClient

object_storage_path_prefix = ['s3://', 'gs://']


class ObjectStorage(object):

    @staticmethod
    def if_object_storage_path(path: str) -> bool:
        for prefix in object_storage_path_prefix:
            if path.startswith(prefix):
                return True
        return False

    @staticmethod
    def get_object_storage_target(path: str, format: Format) -> luigi.Target:
        if path.startswith('s3://'):
            return luigi.contrib.s3.S3Target(path, client=S3Config().get_s3_client(), format=format)
        elif path.startswith('gs://'):
            return luigi.contrib.gcs.GCSTarget(path, client=GCSConfig().get_gcs_client(), format=format)
        else:
            raise

    @staticmethod
    def exists(path: str) -> bool:
        if path.startswith('s3://'):
            return S3Config().get_s3_client().exists(path)
        elif path.startswith('gs://'):
            return GCSConfig().get_gcs_client().exists(path)
        else:
            raise

    @staticmethod
    def get_timestamp(path: str) -> datetime:
        if path.startswith('s3://'):
            return S3Config().get_s3_client().get_key(path).last_modified
        elif path.startswith('gs://'):
            # for gcs object
            # should PR to luigi
            bucket, obj = GCSConfig().get_gcs_client()._path_to_bucket_and_key(path)
            result = GCSConfig().get_gcs_client().client.objects().get(bucket=bucket, object=obj).execute()
            return result['updated']
        else:
            raise

    @staticmethod
    def get_zip_client(file_path: str, temporary_directory: str) -> ZipClient:
        if file_path.startswith('s3://'):
            return S3ZipClient(file_path=file_path, temporary_directory=temporary_directory)
        elif file_path.startswith('gs://'):
            return GCSZipClient(file_path=file_path, temporary_directory=temporary_directory)
        else:
            raise

    @staticmethod
    def is_buffered_reader(file: object):
        return not isinstance(file, luigi.contrib.s3.ReadableS3File)
