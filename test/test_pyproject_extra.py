import unittest

import pytest


class TestPyprojectExtra(unittest.TestCase):

    @pytest.mark.gcs
    def test_gcs_installed(self):
        try:
            import googleapiclient  # noqa: F401
        except ImportError:
            raise Exception('googleapiclient should be installed')

    @pytest.mark.no_gcs
    def test_no_gcs(self):
        try:
            import googleapiclient  # noqa: F401
            raise Exception('googleapiclient should not be installed')
        except ImportError:
            pass

    @pytest.mark.s3
    def test_s3_installed(self):
        try:
            import boto3  # noqa: F401
        except ImportError:
            raise Exception('boto3 should be installed')

    @pytest.mark.no_s3
    def test_no_s3(self):
        try:
            import boto3  # noqa: F401
            raise Exception('boto3 should not be installed')
        except ImportError:
            pass
