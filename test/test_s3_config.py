import unittest

import pytest

try:
    from gokart.s3_config import S3Config
except ImportError:
    pass


@pytest.mark.s3
class TestS3Config(unittest.TestCase):

    def test_get_same_s3_client(self):
        client_a = S3Config().get_s3_client()
        client_b = S3Config().get_s3_client()

        self.assertEqual(client_a, client_b)
