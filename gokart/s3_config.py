from __future__ import annotations

import os
from types import ModuleType
from typing import TYPE_CHECKING, Any, cast

import luigi

from gokart._optional_dependency import import_optional_dependency
from gokart.conflict_prevention_lock.lock_config import LOCK_REQUEST_RETRY_DEADLINE_SECONDS, LOCK_REQUEST_TIMEOUT_SECONDS
from gokart.process_cache import cached_per_process

if TYPE_CHECKING:
    from luigi.contrib.s3 import S3Client


_MIN_BOTOCORE_FOR_CONDITIONAL_WRITES = (1, 35, 69)
_LOCK_CONNECT_TIMEOUT_SECONDS = 5


def import_s3_dependency(module_name: str) -> ModuleType:
    return import_optional_dependency(module_name, extra='s3', feature='S3')


class S3Config(luigi.Config):
    aws_access_key_id_name: luigi.Parameter[str] = luigi.Parameter(default='AWS_ACCESS_KEY_ID', description='AWS access key id environment variable.')
    aws_secret_access_key_name: luigi.Parameter[str] = luigi.Parameter(
        default='AWS_SECRET_ACCESS_KEY', description='AWS secret access key environment variable.'
    )

    endpoint_url: luigi.OptionalParameter = luigi.OptionalParameter(
        default=None, description='Endpoint URL of an S3-compatible store (e.g. MinIO, R2), used by S3 targets and the S3 run lock. AWS S3 is used when None.'
    )

    _client: tuple[int, S3Client] | None = None

    def get_s3_client(self) -> S3Client:
        # boto3 sessions and clients are not fork-safe
        self._client = cached_per_process(self._client, self._get_s3_client)
        return self._client[1]

    def _get_s3_client(self) -> S3Client:
        return self._new_luigi_client()

    def _new_luigi_client(self, **extra: Any) -> S3Client:
        import_s3_dependency('boto3')
        from luigi.contrib.s3 import S3Client

        return S3Client(
            aws_access_key_id=os.environ.get(self.aws_access_key_id_name),
            aws_secret_access_key=os.environ.get(self.aws_secret_access_key_name),
            **self._endpoint_kwargs(),
            **extra,
        )

    def _endpoint_kwargs(self) -> dict[str, str]:
        return {'endpoint_url': self.endpoint_url} if self.endpoint_url else {}

    def create_boto_client(self) -> Any:
        """Build a boto3 S3 client that is not shared with anyone else, e.g. for a background thread.

        It is derived from a new luigi ``S3Client``, so it honours the same options as the S3 targets: the ``[s3]`` section of luigi
        (``endpoint_url``, ``region_name``, ``aws_session_token``, ``aws_role_arn`` / ``aws_role_session_name``, ...) as well as this section.
        """
        _check_botocore_supports_conditional_writes()
        from botocore.config import Config

        # The heartbeat thread extends every held lock one after another, so one hung request must not outlast the lock timeout: botocore's
        # defaults (60 s timeouts, 5 attempts) would allow about 300 s. The attempts fit into the 30 s retry deadline of the GCS store.
        attempt_seconds = _LOCK_CONNECT_TIMEOUT_SECONDS + LOCK_REQUEST_TIMEOUT_SECONDS
        client = self._new_luigi_client(
            config=Config(
                connect_timeout=_LOCK_CONNECT_TIMEOUT_SECONDS,
                read_timeout=LOCK_REQUEST_TIMEOUT_SECONDS,
                retries={'mode': 'standard', 'total_max_attempts': max(1, LOCK_REQUEST_RETRY_DEADLINE_SECONDS // attempt_seconds)},
            )
        )
        return cast(Any, client).s3.meta.client


def _check_botocore_supports_conditional_writes() -> None:
    botocore = import_s3_dependency('botocore')

    version = tuple(int(part) for part in botocore.__version__.split('.')[:3] if part.isdigit())
    # Older botocore rejects IfMatch / IfNoneMatch on put_object and delete_object with ParamValidationError.
    if version < _MIN_BOTOCORE_FOR_CONDITIONAL_WRITES:
        raise ImportError(
            f'The S3 run lock needs botocore>={".".join(map(str, _MIN_BOTOCORE_FOR_CONDITIONAL_WRITES))} for conditional writes, but {botocore.__version__} is installed. '
            'Install a newer one with: pip install gokart[s3]'
        )
