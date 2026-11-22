from __future__ import annotations

import importlib.abc
import importlib.machinery
import importlib.util
import sys
from collections.abc import Callable
from types import ModuleType
from typing import Any

import luigi.format
import pytest

BlockModules = Callable[..., None]


@pytest.fixture
def block_modules(monkeypatch: pytest.MonkeyPatch) -> BlockModules:
    """Make importing the given modules fail as when they are not installed (ModuleNotFoundError with name set)."""

    def _block(*names: str) -> None:
        for name in names:
            monkeypatch.setitem(sys.modules, name, None)

    return _block


class _BrokenModuleFinder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, name: str, error: ImportError) -> None:
        self._name = name
        self._error = error

    def find_spec(self, fullname: str, path: Any, target: ModuleType | None = None) -> importlib.machinery.ModuleSpec | None:
        return importlib.util.spec_from_loader(fullname, self) if fullname == self._name else None

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> None:
        return None

    def exec_module(self, module: ModuleType) -> None:
        raise self._error


BreakModule = Callable[[str, ImportError], None]


@pytest.fixture
def break_module(monkeypatch: pytest.MonkeyPatch) -> BreakModule:
    """Make importing the given module fail as when it is installed but one of its own imports fails."""

    def _break(name: str, error: ImportError) -> None:
        monkeypatch.delitem(sys.modules, name, raising=False)
        monkeypatch.setattr(sys, 'meta_path', [_BrokenModuleFinder(name, error), *sys.meta_path])

    return _break


class TestS3ExtrasNotInstalled:
    def test_s3_config_raises_import_error_without_boto3(self, block_modules: BlockModules):
        from gokart.s3_config import S3Config

        config = S3Config()
        config._client = None

        block_modules('boto3')
        with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
            config._get_s3_client()

    def test_object_storage_get_target_raises_for_s3_without_extras(self, block_modules: BlockModules):
        from gokart.object_storage import ObjectStorage

        block_modules('boto3')
        with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
            ObjectStorage.get_object_storage_target('s3://bucket/key', format=luigi.format.Nop)

    def test_object_storage_exists_raises_for_s3_without_extras(self, block_modules: BlockModules):
        from gokart.object_storage import ObjectStorage

        block_modules('boto3')
        with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
            ObjectStorage.exists('s3://bucket/key')

    def test_object_storage_get_zip_client_raises_for_s3_without_extras(self, block_modules: BlockModules, tmp_path):
        from gokart.object_storage import ObjectStorage

        block_modules('boto3')
        with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
            ObjectStorage.get_zip_client('s3://bucket/key.zip', str(tmp_path))

    def test_s3_zip_client_raises_without_extras(self, block_modules: BlockModules, tmp_path):
        from gokart.s3_zip_client import S3ZipClient

        block_modules('boto3')
        with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
            S3ZipClient(file_path='s3://bucket/key.zip', temporary_directory=str(tmp_path))

    def test_object_storage_is_buffered_reader_returns_true_without_s3(self, block_modules: BlockModules):
        from gokart.object_storage import ObjectStorage

        block_modules('boto3', 'luigi.contrib.s3')
        assert ObjectStorage.is_buffered_reader(object()) is True

    def test_s3_config_create_boto_client_raises_import_error_without_botocore(self, block_modules: BlockModules):
        from gokart.s3_config import S3Config

        block_modules('boto3', 'botocore')
        with pytest.raises(ImportError, match=r'pip install gokart\[s3\]'):
            S3Config().create_boto_client()


class TestGCSExtrasNotInstalled:
    def test_gcs_config_raises_import_error_without_gcs_lib(self, block_modules: BlockModules):
        from gokart.gcs_config import GCSConfig

        config = GCSConfig()
        config._client = None

        block_modules('googleapiclient')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            config._get_gcs_client()

    def test_gcs_config_load_credentials_raises_without_google_auth(self, block_modules: BlockModules):
        from gokart.gcs_config import GCSConfig

        config = GCSConfig()

        block_modules('google.oauth2.service_account')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            config._load_oauth_credentials()

    def test_gcs_config_get_storage_client_raises_without_gcs_lib(self, block_modules: BlockModules):
        from gokart.gcs_config import GCSConfig

        GCSConfig.clear_instance_cache()
        try:
            block_modules('google.cloud.storage')
            with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
                GCSConfig().get_storage_client()
        finally:
            GCSConfig.clear_instance_cache()

    def test_object_storage_exists_raises_for_gcs_without_extras(self, block_modules: BlockModules):
        from gokart.object_storage import ObjectStorage

        block_modules('google.cloud.storage')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            ObjectStorage.exists('gs://bucket/key')

    def test_object_storage_get_zip_client_raises_for_gcs_without_extras(self, block_modules: BlockModules, tmp_path):
        from gokart.object_storage import ObjectStorage

        block_modules('google.cloud.storage')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            ObjectStorage.get_zip_client('gs://bucket/key.zip', str(tmp_path))

    def test_gcs_zip_client_raises_without_extras(self, block_modules: BlockModules, tmp_path):
        from gokart.gcs_zip_client import GCSZipClient

        block_modules('google.cloud.storage')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            GCSZipClient(file_path='gs://bucket/key.zip', temporary_directory=str(tmp_path))

    def test_object_storage_get_target_raises_for_gcs_without_extras(self, block_modules: BlockModules):
        from gokart.object_storage import ObjectStorage

        block_modules('google.cloud.storage')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            ObjectStorage.get_object_storage_target('gs://bucket/key', format=luigi.format.Nop)


def _new_s3_client() -> object:
    from gokart.s3_config import S3Config

    return S3Config()._new_luigi_client()


def _get_s3_target() -> object:
    from gokart.object_storage import ObjectStorage

    return ObjectStorage.get_object_storage_target('s3://bucket/key', format=luigi.format.Nop)


def _get_gcs_storage_client() -> object:
    from gokart.gcs_config import GCSConfig

    GCSConfig.clear_instance_cache()
    try:
        return GCSConfig().get_storage_client()
    finally:
        GCSConfig.clear_instance_cache()


def _get_gcs_client() -> object:
    from gokart.gcs_config import GCSConfig

    return GCSConfig()._get_gcs_client()


def _load_gcs_credentials() -> object:
    from gokart.gcs_config import GCSConfig

    return GCSConfig()._load_oauth_credentials()


_EXTRA_IMPORTS = [
    pytest.param(_new_s3_client, 'boto3', id='s3_config-new-luigi-client'),
    pytest.param(_get_s3_target, 'boto3', id='object_storage-get-s3-target'),
    pytest.param(_get_gcs_storage_client, 'google.cloud.storage', id='gcs_config-import-storage'),
    pytest.param(_get_gcs_client, 'googleapiclient', id='gcs_config-get-gcs-client'),
    pytest.param(_load_gcs_credentials, 'google.oauth2.service_account', id='gcs_config-load-oauth-credentials'),
]


class TestExtrasInstalledButBroken:
    """The install hint would be wrong when the extra is installed but fails to import, and would hide the real cause."""

    @pytest.mark.parametrize(
        'error',
        [
            pytest.param(ModuleNotFoundError("No module named '_gokart_missing_dependency'", name='_gokart_missing_dependency'), id='missing-dependency'),
            pytest.param(ImportError('broken dependency'), id='import-error'),
        ],
    )
    @pytest.mark.parametrize(('call', 'module_name'), _EXTRA_IMPORTS)
    def test_keeps_original_error(self, break_module: BreakModule, call: Callable[[], object], module_name: str, error: ImportError):
        break_module(module_name, error)
        with pytest.raises(type(error)) as exc_info:
            call()
        assert exc_info.value is error


class TestIsBufferedReaderWithBrokenBoto3:
    def test_local_file_does_not_need_boto3(self, break_module: BreakModule):
        # A local load never touches S3, so a broken boto3 must not break it.
        from gokart.object_storage import ObjectStorage

        break_module('boto3', ImportError('broken dependency'))
        assert ObjectStorage.is_buffered_reader(object()) is True


def test_is_buffered_reader_returns_false_for_s3_reader() -> None:
    import luigi.contrib.s3

    from gokart.object_storage import ObjectStorage

    class _S3Object:
        def get(self) -> dict[str, object]:
            return {'Body': object()}

    assert ObjectStorage.is_buffered_reader(luigi.contrib.s3.ReadableS3File(_S3Object())) is False


class TestExtrasParentPackageNotInstalled:
    def test_gcs_raises_install_hint_without_google_cloud(self, block_modules: BlockModules, monkeypatch: pytest.MonkeyPatch):
        # google.cloud is a namespace package shared with other google-cloud-* distributions, so it can be missing as a whole.
        monkeypatch.delitem(sys.modules, 'google.cloud.storage', raising=False)
        block_modules('google.cloud')
        with pytest.raises(ImportError, match=r'pip install gokart\[gcs\]'):
            _get_gcs_storage_client()


class TestExtrasInstalled:
    """Verify that imports succeed when extras are installed (current test environment)."""

    def test_s3_config_can_be_imported(self):
        from gokart.s3_config import S3Config

        assert S3Config is not None

    def test_gcs_config_can_be_imported(self):
        from gokart.gcs_config import GCSConfig

        assert GCSConfig is not None

    def test_object_storage_can_be_imported(self):
        from gokart.object_storage import ObjectStorage

        assert ObjectStorage is not None

    def test_luigi_contrib_s3_is_importable(self):
        import luigi.contrib.s3

        assert luigi.contrib.s3 is not None

    def test_luigi_contrib_gcs_is_importable(self):
        import luigi.contrib.gcs

        assert luigi.contrib.gcs is not None
