from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, cast

import luigi

if TYPE_CHECKING:
    import luigi.contrib.s3


_MIN_BOTOCORE_FOR_CONDITIONAL_WRITES = (1, 35, 69)


class S3Config(luigi.Config):
    aws_access_key_id_name: luigi.Parameter[str] = luigi.Parameter(default='AWS_ACCESS_KEY_ID', description='AWS access key id environment variable.')
    aws_secret_access_key_name: luigi.Parameter[str] = luigi.Parameter(
        default='AWS_SECRET_ACCESS_KEY', description='AWS secret access key environment variable.'
    )

    endpoint_url: luigi.OptionalParameter = luigi.OptionalParameter(
        default=None, description='Endpoint URL of an S3-compatible store (e.g. MinIO, R2), used by S3 targets and the S3 run lock. AWS S3 is used when None.'
    )

    _client: luigi.contrib.s3.S3Client | None = None

    def get_s3_client(self) -> luigi.contrib.s3.S3Client:
        if self._client is None:  # use cache as like singleton object
            self._client = self._get_s3_client()
        return self._client

    def _get_s3_client(self) -> luigi.contrib.s3.S3Client:
        return self._new_luigi_client()

    def _new_luigi_client(self) -> luigi.contrib.s3.S3Client:
        try:
            import boto3  # noqa: F401  # pyright: ignore[reportUnusedImport]
        except ImportError:
            raise ImportError('S3 support requires additional dependencies. Install them with: pip install gokart[s3]') from None
        import luigi.contrib.s3

        return luigi.contrib.s3.S3Client(
            aws_access_key_id=os.environ.get(self.aws_access_key_id_name),
            aws_secret_access_key=os.environ.get(self.aws_secret_access_key_name),
            **self._endpoint_kwargs(),
        )

    def _endpoint_kwargs(self) -> dict[str, str]:
        return {'endpoint_url': self.endpoint_url} if self.endpoint_url else {}

    def create_boto_client(self) -> Any:
        """Build a boto3 S3 client that is not shared with anyone else, e.g. for a background thread.

        It is derived from a new luigi ``S3Client``, so it honours the same options as the S3 targets: the ``[s3]`` section of luigi
        (``endpoint_url``, ``region_name``, ``aws_session_token``, ``aws_role_arn`` / ``aws_role_session_name``, ...) as well as this section.
        """
        client = self._new_luigi_client()
        _check_botocore_supports_conditional_writes()
        return cast(Any, client).s3.meta.client


def _check_botocore_supports_conditional_writes() -> None:
    import botocore

    version = tuple(int(part) for part in botocore.__version__.split('.')[:3] if part.isdigit())
    # Older botocore rejects IfMatch / IfNoneMatch on put_object and delete_object with ParamValidationError.
    if version < _MIN_BOTOCORE_FOR_CONDITIONAL_WRITES:
        raise ImportError(
            f'The S3 run lock needs botocore>={".".join(map(str, _MIN_BOTOCORE_FOR_CONDITIONAL_WRITES))} for conditional writes, but {botocore.__version__} is installed. '
            'Install a newer one with: pip install gokart[s3]'
        )
