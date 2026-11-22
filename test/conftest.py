import os
import socket
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolated_working_directory(tmp_path: Path) -> Iterator[None]:
    # TaskOnKart's default workspace_directory is './resources/', so tests that don't set it would leave files in the repository
    original = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield
    finally:
        os.chdir(original)


@pytest.fixture(autouse=True)
def _clear_s3_config_cache() -> Iterator[None]:
    # A client cached on an S3Config by one test would otherwise make a later test skip the boto3 import, which depends on the test order.
    from gokart.s3_config import S3Config

    S3Config.clear_instance_cache()
    yield
    S3Config.clear_instance_cache()


@pytest.fixture(scope='session')
def _gcs_emulator_host() -> Iterator[str]:
    from gcp_storage_emulator.server import create_server  # pyright: ignore[reportMissingTypeStubs]

    # each pytest-xdist worker needs its own port, and the emulator does not expose the port it bound to
    with socket.socket() as sock:
        sock.bind(('localhost', 0))
        port = sock.getsockname()[1]
    server = create_server('localhost', port, in_memory=True)
    server.start()
    try:
        yield f'http://localhost:{port}'
    finally:
        server.stop()


@pytest.fixture
def gcs_bucket(_gcs_emulator_host: str, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Point google-cloud-storage at an in-process GCS emulator and return the name of an empty bucket."""
    from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]

    from gokart.gcs_config import GCSConfig

    monkeypatch.setenv('STORAGE_EMULATOR_HOST', _gcs_emulator_host)
    # GCSConfig instances (and the clients cached on them) are shared through luigi's instance cache
    GCSConfig.clear_instance_cache()
    bucket_name = f'test-{uuid.uuid4().hex}'
    storage.Client().create_bucket(bucket_name)
    yield bucket_name
    GCSConfig.clear_instance_cache()
