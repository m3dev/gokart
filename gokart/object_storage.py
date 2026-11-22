from __future__ import annotations

import sys
from datetime import datetime
from typing import Any, cast

import luigi
from luigi.format import Format

from gokart._optional_dependency import import_optional_dependency
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
            import_optional_dependency('boto3', extra='s3', feature='S3')

            import luigi.contrib.s3

            from gokart.s3_config import S3Config

            return luigi.contrib.s3.S3Target(path, client=S3Config().get_s3_client(), format=format)
        elif path.startswith('gs://'):
            from gokart._gcs import GCSTarget

            return GCSTarget(path, format=format)
        else:
            raise

    @staticmethod
    def exists(path: str) -> bool:
        if path.startswith('s3://'):
            from gokart.s3_config import S3Config

            return cast(bool, cast(Any, S3Config().get_s3_client()).exists(path))
        elif path.startswith('gs://'):
            from gokart._gcs import GCSClient

            return GCSClient().exists(path)
        else:
            raise

    @staticmethod
    def get_timestamp(path: str) -> datetime:
        if path.startswith('s3://'):
            from gokart.s3_config import S3Config

            key = cast(Any, S3Config().get_s3_client()).get_key(path)
            if key is None:
                raise FileNotFoundError(f'S3 key not found: {path}')
            return cast(datetime, key.last_modified)
        elif path.startswith('gs://'):
            from gokart._gcs import GCSClient

            return GCSClient().get_timestamp(path)
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
        # A ReadableS3File can only exist once luigi.contrib.s3 has been imported, so there is no need to import boto3 here: that would
        # slow down the first local load, and a broken boto3 would break loading local files that never touch S3.
        s3 = sys.modules.get('luigi.contrib.s3')
        return s3 is None or not isinstance(file, s3.ReadableS3File)
