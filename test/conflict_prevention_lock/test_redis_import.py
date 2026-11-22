import subprocess
import sys
import textwrap
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import luigi
import luigi.configuration
import pytest
from luigi.task_register import Register

import gokart
from gokart.conflict_prevention_lock.redis_import import import_redis
from gokart.conflict_prevention_lock.run_lock import RedisRunLock
from gokart.conflict_prevention_lock.task_lock import RedisClient, TaskLockParams


@pytest.fixture
def without_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    # a None entry in sys.modules makes `import redis` raise ModuleNotFoundError with name='redis', as when redis is not installed
    monkeypatch.setitem(sys.modules, 'redis', None)


def test_import_redis_returns_module() -> None:
    assert import_redis().__name__ == 'redis'


@pytest.mark.usefixtures('without_redis')
def test_import_redis_raises_install_hint() -> None:
    with pytest.raises(ImportError, match=r'pip install gokart\[redis\]'):
        import_redis()


@pytest.mark.parametrize(
    ('redis_init', 'expected_error', 'expected_message'),
    [
        pytest.param('import _gokart_missing_redis_dependency', ModuleNotFoundError, '_gokart_missing_redis_dependency', id='missing-dependency'),
        pytest.param('import redis._gokart_missing_submodule', ModuleNotFoundError, r'redis\._gokart_missing_submodule', id='missing-submodule'),
        pytest.param("raise ImportError('broken redis dependency')", ImportError, 'broken redis dependency', id='import-error'),
    ],
)
def test_import_redis_keeps_original_error_when_installed_redis_fails_to_import(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, redis_init: str, expected_error: type[ImportError], expected_message: str
) -> None:
    # The install hint would be wrong here and would hide the real cause.
    package = tmp_path / 'redis'
    package.mkdir()
    (package / '__init__.py').write_text(redis_init)
    monkeypatch.delitem(sys.modules, 'redis', raising=False)
    monkeypatch.syspath_prepend(str(tmp_path))

    with pytest.raises(expected_error, match=expected_message) as exc_info:
        import_redis()
    assert 'gokart[redis]' not in str(exc_info.value)


@pytest.mark.usefixtures('without_redis')
def test_redis_client_raises_install_hint() -> None:
    with pytest.raises(ImportError, match=r'pip install gokart\[redis\]'):
        RedisClient(host='without-redis', port=6379)


@pytest.mark.usefixtures('without_redis')
def test_redis_run_lock_raises_install_hint() -> None:
    params = TaskLockParams('localhost', 6379, 10, 'key', True, False, 1)
    with pytest.raises(ImportError, match=r'pip install gokart\[redis\]'):
        RedisRunLock(params)


def test_gokart_is_importable_without_redis() -> None:
    code = textwrap.dedent(
        """
        import sys
        sys.modules['redis'] = None
        import gokart
        import gokart.task
        """
    )
    subprocess.run([sys.executable, '-c', code], check=True)


class _LockConfiguredTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    random_id: luigi.StrParameter = luigi.StrParameter()

    def run(self):
        self.dump('done')


@pytest.mark.usefixtures('without_redis')
@pytest.mark.parametrize('should_lock_run', [False, True])
def test_task_with_redis_host_and_port_raises_install_hint_at_construction(should_lock_run: bool) -> None:
    # Failing here, before run(), keeps a missing extra from wasting a whole run() that would only fail at dump().
    with pytest.raises(ImportError, match=r'pip install gokart\[redis\]'):
        _LockConfiguredTask(random_id=uuid.uuid4().hex, redis_host='localhost', redis_port=6379, should_lock_run=should_lock_run)


@pytest.mark.usefixtures('without_redis')
def test_task_without_both_redis_host_and_port_does_not_need_redis() -> None:
    # Locks are taken only when both are set, so redis is not needed otherwise.
    _LockConfiguredTask(random_id=uuid.uuid4().hex, redis_host='localhost')
    _LockConfiguredTask(random_id=uuid.uuid4().hex, redis_port=6379)


@pytest.mark.usefixtures('without_redis')
def test_build_without_redis(tmp_path: Path) -> None:
    task = _LockConfiguredTask(random_id=uuid.uuid4().hex, workspace_directory=str(tmp_path))
    assert gokart.build(task, reset_register=False) == 'done'


@pytest.fixture
def redis_lock_url() -> Iterator[None]:
    config = luigi.configuration.get_config()
    existed = config.has_section('gokart_lock')
    if not existed:
        config.add_section('gokart_lock')
    previous = cast(str | None, config.get('gokart_lock', 'url', fallback=None))
    config.set('gokart_lock', 'url', 'redis://localhost:6379')
    # gokart_lock() is a cached luigi.Config instance, so it would not see the new url
    Register.clear_instance_cache()
    yield
    if not existed:
        config.remove_section('gokart_lock')
    elif previous is None:
        config.remove_option('gokart_lock', 'url')
    else:
        config.set('gokart_lock', 'url', previous)
    Register.clear_instance_cache()


@pytest.mark.usefixtures('without_redis', 'redis_lock_url')
@pytest.mark.parametrize('should_lock_run', [False, True])
def test_task_with_redis_lock_url_raises_install_hint_at_construction(should_lock_run: bool) -> None:
    with pytest.raises(ImportError, match=r'pip install gokart\[redis\]'):
        _LockConfiguredTask(random_id=uuid.uuid4().hex, should_lock_run=should_lock_run)
