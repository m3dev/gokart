import unittest
from collections.abc import Callable, Iterator
from unittest.mock import patch

import luigi.configuration
import pytest

from gokart.s3_config import S3Config


class TestS3Config(unittest.TestCase):
    def test_get_same_s3_client(self):
        client_a = S3Config().get_s3_client()
        client_b = S3Config().get_s3_client()

        self.assertEqual(client_a, client_b)


@pytest.fixture
def luigi_s3_section() -> Iterator[Callable[..., None]]:
    config = luigi.configuration.get_config()
    config.add_section('s3')

    def _set(**values: str) -> None:
        for name, value in values.items():
            config.set('s3', name, value)

    yield _set
    config.remove_section('s3')


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('AWS_ACCESS_KEY_ID', 'testing')
    monkeypatch.setenv('AWS_SECRET_ACCESS_KEY', 'testing')
    monkeypatch.setenv('AWS_DEFAULT_REGION', 'us-east-1')


class TestS3ConfigEndpointUrl:
    def test_endpoint_url_defaults_to_none(self):
        assert S3Config().endpoint_url is None

    def test_boto_client_gets_the_endpoint_url(self):
        assert S3Config(endpoint_url='http://localhost:9000').create_boto_client().meta.endpoint_url == 'http://localhost:9000'

    def test_boto_client_without_endpoint_url_uses_aws(self):
        assert S3Config().create_boto_client().meta.endpoint_url.endswith('amazonaws.com')

    def test_boto_client_limits_how_long_one_request_can_block_the_heartbeat(self):
        config = S3Config().create_boto_client().meta.config
        assert (config.connect_timeout, config.read_timeout) == (5, 10)
        assert config.retries['total_max_attempts'] == 2

    def test_each_boto_client_is_dedicated(self):
        config = S3Config()
        assert config.create_boto_client() is not config.create_boto_client()

    def test_boto_client_honours_the_luigi_s3_section_like_the_targets(self, luigi_s3_section: Callable[..., None]):
        luigi_s3_section(endpoint_url='http://legacy:9000', region_name='eu-west-1')
        client = S3Config().create_boto_client()
        assert (client.meta.endpoint_url, client.meta.region_name) == ('http://legacy:9000', 'eu-west-1')

    def test_the_s3_config_endpoint_wins_over_the_luigi_s3_section(self, luigi_s3_section: Callable[..., None]):
        luigi_s3_section(endpoint_url='http://legacy:9000')
        assert S3Config(endpoint_url='http://localhost:9002').create_boto_client().meta.endpoint_url == 'http://localhost:9002'

    def test_an_old_botocore_is_reported_with_the_extra_to_install(self):
        with patch('botocore.__version__', '1.35.0'), pytest.raises(ImportError, match=r'botocore>=1\.35\.69.*gokart\[s3\]'):
            S3Config().create_boto_client()

    def test_luigi_client_gets_the_endpoint_url(self):
        with patch('luigi.contrib.s3.S3Client') as client:
            S3Config(endpoint_url='http://localhost:9001').get_s3_client()
        assert client.call_args.kwargs['endpoint_url'] == 'http://localhost:9001'

    def test_luigi_client_without_endpoint_url_keeps_the_default(self):
        with patch('luigi.contrib.s3.S3Client') as client:
            S3Config(aws_access_key_id_name='AWS_ACCESS_KEY_ID_FOR_TEST').get_s3_client()
        assert 'endpoint_url' not in client.call_args.kwargs
