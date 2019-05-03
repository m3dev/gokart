import os
import shutil
import unittest

import boto3
from moto import mock_s3

from gokart.zip_client import S3ZipClient


def _get_temporary_directory():
    return os.path.abspath(os.path.join(os.path.dirname(__name__), 'temporary'))


class TestS3ZipClient(unittest.TestCase):
    def tearDown(self):
        shutil.rmtree(_get_temporary_directory(), ignore_errors=True)

    @mock_s3
    def test_make_archive(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        file_path = os.path.join('s3://test/', 'test.zip')
        temporary_directory = _get_temporary_directory()

        zip_client = S3ZipClient(file_path=file_path, temporary_directory=temporary_directory)
        # raise error if temporary directory does not exist.
        with self.assertRaises(FileNotFoundError):
            zip_client.make_archive()

        # run without error because temporary directory exists.
        os.makedirs(temporary_directory, exist_ok=True)
        zip_client.make_archive()

    @mock_s3
    def test_unpack_archive(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        file_path = os.path.join('s3://test/', 'test.zip')
        in_temporary_directory = os.path.join(_get_temporary_directory(), 'in', 'dummy')
        out_temporary_directory = os.path.join(_get_temporary_directory(), 'out', 'dummy')

        # make dummy zip file.
        os.makedirs(in_temporary_directory, exist_ok=True)
        in_zip_client = S3ZipClient(file_path=file_path, temporary_directory=in_temporary_directory)
        in_zip_client.make_archive()

        # load dummy zip file.
        out_zip_client = S3ZipClient(file_path=file_path, temporary_directory=out_temporary_directory)
        self.assertFalse(os.path.exists(out_temporary_directory))
        out_zip_client.unpack_archive()
