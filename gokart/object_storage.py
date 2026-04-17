from __future__ import annotations

from datetime import datetime
from typing import cast

import luigi
from luigi.format import Format

from gokart.zip_client import ZipClient

object_storage_path_prefix = ['s3://', 'gs://']


class ObjectStorage:
    @staticmethod
    def if_object_storage_path(path: str) -> bool:
        for prefix in object_storage_path_prefix:
            if path.startswith(prefix):
                return True
        return False

    @staticmethod
    def get_object_storage_target(path: str, format: Format) -> luigi.target.FileSystemTarget:
        if path.startswith('s3://'):
            try:
                import luigi.contrib.s3
            except ImportError:
                raise ImportError('S3 support requires additional dependencies. Install them with: pip install gokart[s3]') from None

            from gokart.s3_config import S3Config

            return luigi.contrib.s3.S3Target(path, client=S3Config().get_s3_client(), format=format)
        elif path.startswith('gs://'):
            try:
                import luigi.contrib.gcs
            except ImportError:
                raise ImportError('GCS support requires additional dependencies. Install them with: pip install gokart[gcs]') from None

            from gokart.gcs_config import GCSConfig

            return luigi.contrib.gcs.GCSTarget(path, client=GCSConfig().get_gcs_client(), format=format)
        else:
            raise

    @staticmethod
    def exists(path: str) -> bool:
        if path.startswith('s3://'):
            from gokart.s3_config import S3Config

            return cast(bool, S3Config().get_s3_client().exists(path))
        elif path.startswith('gs://'):
            from gokart.gcs_config import GCSConfig

            return cast(bool, GCSConfig().get_gcs_client().exists(path))
        else:
            raise

    @staticmethod
    def get_timestamp(path: str) -> datetime:
        if path.startswith('s3://'):
            from gokart.s3_config import S3Config

            return cast(datetime, S3Config().get_s3_client().get_key(path).last_modified)
        elif path.startswith('gs://'):
            from gokart.gcs_config import GCSConfig

            # for gcs object
            # should PR to luigi
            bucket, obj = GCSConfig().get_gcs_client()._path_to_bucket_and_key(path)
            result = GCSConfig().get_gcs_client().client.objects().get(bucket=bucket, object=obj).execute()
            return cast(datetime, result['updated'])
        else:
            raise

    @staticmethod
    def get_zip_client(file_path: str, temporary_directory: str) -> ZipClient:
        if file_path.startswith('s3://'):
            from gokart.s3_zip_client import S3ZipClient

            return S3ZipClient(file_path=file_path, temporary_directory=temporary_directory)
        elif file_path.startswith('gs://'):
            from gokart.gcs_zip_client import GCSZipClient

            return GCSZipClient(file_path=file_path, temporary_directory=temporary_directory)
        else:
            raise

    @staticmethod
    def is_buffered_reader(file: object) -> bool:
        try:
            import luigi.contrib.s3

            return not isinstance(file, luigi.contrib.s3.ReadableS3File)
        except ImportError:
            return True
