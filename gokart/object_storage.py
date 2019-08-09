from datetime import datetime

import luigi
import luigi.contrib.s3
from gokart.s3_config import S3Config
from gokart.zip_client import ZipClient
from gokart.s3_zip_client import S3ZipClient


object_storage_path_prefix = [
    's3://'
]


def if_object_storage_path(path: str) -> bool:
    for prefix in object_storage_path_prefix:
        if path.startswith(prefix):
            return True
    return False


def get_object_storage_target(path: str, format: luigi.Format) -> luigi.Target:
    return luigi.contrib.s3.S3Target(path, client=S3Config().get_s3_client(), format=format)


def exists(path: str) -> bool:
    return S3Config().get_s3_client().exists(path)


def get_timestamp(path: str) -> datetime:
    return S3Config().get_s3_client().get_key(path).last_modified


def get_zip_client(file_path: str, temporary_directory: str) -> ZipClient:
    return S3ZipClient(file_path=file_path, temporary_directory=temporary_directory)


def is_readable_objectstorage_instance(file: object):
    return isinstance(file, luigi.contrib.s3.ReadableS3File)
