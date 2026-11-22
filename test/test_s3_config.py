import unittest
from unittest.mock import patch

from gokart.s3_config import S3Config


class TestS3Config(unittest.TestCase):
    def test_get_same_s3_client(self):
        client_a = S3Config().get_s3_client()
        client_b = S3Config().get_s3_client()

        self.assertEqual(client_a, client_b)


class TestS3ConfigEndpointUrl:
    def test_endpoint_url_defaults_to_none(self):
        assert S3Config().endpoint_url is None

    def test_boto_client_gets_the_endpoint_url(self):
        with patch('boto3.client') as client:
            S3Config(endpoint_url='http://localhost:9000').create_boto_client()
        assert client.call_args.kwargs['endpoint_url'] == 'http://localhost:9000'

    def test_boto_client_without_endpoint_url_keeps_the_default(self):
        with patch('boto3.client') as client:
            S3Config().create_boto_client()
        assert 'endpoint_url' not in client.call_args.kwargs

    def test_luigi_client_gets_the_endpoint_url(self):
        with patch('luigi.contrib.s3.S3Client') as client:
            S3Config(endpoint_url='http://localhost:9001').get_s3_client()
        assert client.call_args.kwargs['endpoint_url'] == 'http://localhost:9001'

    def test_luigi_client_without_endpoint_url_keeps_the_default(self):
        with patch('luigi.contrib.s3.S3Client') as client:
            S3Config(aws_access_key_id_name='AWS_ACCESS_KEY_ID_FOR_TEST').get_s3_client()
        assert 'endpoint_url' not in client.call_args.kwargs
