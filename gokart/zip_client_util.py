from gokart.object_storage import ObjectStorage
from gokart.zip_client import LocalZipClient, ZipClient


def make_zip_client(file_path: str, temporary_directory: str) -> ZipClient:
    if ObjectStorage.if_object_storage_path(file_path):
        return ObjectStorage.get_zip_client(file_path=file_path, temporary_directory=temporary_directory)
    return LocalZipClient(file_path=file_path, temporary_directory=temporary_directory)
