from __future__ import annotations

from datetime import datetime
from typing import Any, cast

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
                import boto3  # noqa: F401  # pyright: ignore[reportUnusedImport]
            except ImportError:
                raise ImportError('S3 support requires additional dependencies. Install them with: pip install gokart[s3]') from None

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
        try:
            import boto3  # noqa: F401  # pyright: ignore[reportUnusedImport]
        except ImportError:
            return True
        import luigi.contrib.s3

        return not isinstance(file, luigi.contrib.s3.ReadableS3File)
