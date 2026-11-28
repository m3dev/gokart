import os
import socket
import uuid
from collections.abc import Callable, Iterator
from pathlib import Path

import luigi.configuration
import pytest
from luigi.task_register import Register


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
def _clear_luigi_instance_cache() -> Iterator[None]:
    # luigi shares one instance cache across all Tasks and Configs, so an instance (and e.g. a client cached on an S3Config or GCSConfig)
    # created by one test would otherwise leak into later tests and make them depend on the test order.
    Register.clear_instance_cache()
    yield
    Register.clear_instance_cache()


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
def gcs_bucket(_gcs_emulator_host: str, monkeypatch: pytest.MonkeyPatch) -> str:
    """Point google-cloud-storage at an in-process GCS emulator and return the name of an empty bucket."""
    from google.cloud import storage  # pyright: ignore[reportMissingTypeStubs]

    monkeypatch.setenv('STORAGE_EMULATOR_HOST', _gcs_emulator_host)
    bucket_name = f'test-{uuid.uuid4().hex}'
    storage.Client().create_bucket(bucket_name)
    return bucket_name


@pytest.fixture
def lock_config() -> Iterator[Callable[..., None]]:
    """Set options of the [gokart_lock] section for one test, e.g. lock_config(url='redis://localhost:6379')."""
    config = luigi.configuration.get_config()
    existed = config.has_section('gokart_lock')
    if not existed:
        config.add_section('gokart_lock')
    previous = dict(config.items('gokart_lock'))

    def _set(**values: str) -> None:
        for name, value in values.items():
            config.set('gokart_lock', name, value)

    yield _set
    # restore, so that a section from the developer's luigi.cfg survives the test
    if existed:
        for name in dict(config.items('gokart_lock')):
            if name not in previous:
                config.remove_option('gokart_lock', name)
        for name, value in previous.items():
            config.set('gokart_lock', name, value)
    else:
        config.remove_section('gokart_lock')
