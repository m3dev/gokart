import subprocess
import sys
import textwrap

import pytest

from gokart.conflict_prevention_lock.redis_import import import_redis
from gokart.conflict_prevention_lock.run_lock import RedisRunLock
from gokart.conflict_prevention_lock.task_lock import RedisClient, TaskLockParams


@pytest.fixture
def without_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    # a None entry in sys.modules makes `import redis` raise ImportError
    monkeypatch.setitem(sys.modules, 'redis', None)


def test_import_redis_returns_module() -> None:
    assert import_redis().__name__ == 'redis'


@pytest.mark.usefixtures('without_redis')
def test_import_redis_raises_install_hint() -> None:
    with pytest.raises(ImportError, match=r'pip install gokart\[redis\]'):
        import_redis()


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
