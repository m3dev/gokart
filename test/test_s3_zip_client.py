import os
import shutil
import unittest

import boto3
from moto import mock_aws

from gokart.s3_zip_client import S3ZipClient
from test.util import _get_temporary_directory


class TestS3ZipClient(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = _get_temporary_directory()

    def tearDown(self):
        shutil.rmtree(self.temporary_directory, ignore_errors=True)

        # remove temporary zip archive if exists.
        if os.path.exists(f'{self.temporary_directory}.zip'):
            os.remove(f'{self.temporary_directory}.zip')

    @mock_aws
    def test_make_archive(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        file_path = os.path.join('s3://test/', 'test.zip')
        temporary_directory = self.temporary_directory

        zip_client = S3ZipClient(file_path=file_path, temporary_directory=temporary_directory)
        # raise error if temporary directory does not exist.
        with self.assertRaises(FileNotFoundError):
            zip_client.make_archive()

        # run without error because temporary directory exists.
        os.makedirs(temporary_directory, exist_ok=True)
        zip_client.make_archive()

    @mock_aws
    def test_unpack_archive(self):
        conn = boto3.resource('s3', region_name='us-east-1')
        conn.create_bucket(Bucket='test')

        file_path = os.path.join('s3://test/', 'test.zip')
        in_temporary_directory = os.path.join(self.temporary_directory, 'in', 'dummy')
        out_temporary_directory = os.path.join(self.temporary_directory, 'out', 'dummy')

        # make dummy zip file.
        os.makedirs(in_temporary_directory, exist_ok=True)
        in_zip_client = S3ZipClient(file_path=file_path, temporary_directory=in_temporary_directory)
        in_zip_client.make_archive()

        # load dummy zip file.
        out_zip_client = S3ZipClient(file_path=file_path, temporary_directory=out_temporary_directory)
        self.assertFalse(os.path.exists(out_temporary_directory))
        out_zip_client.unpack_archive()
