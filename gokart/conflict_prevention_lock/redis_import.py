from __future__ import annotations

from types import ModuleType

_MISSING_REDIS_EXTRA_MESSAGE = 'Redis lock support requires additional dependencies. Install them with: pip install gokart[redis]'


def import_redis() -> ModuleType:
    try:
        import redis
    except ImportError:
        raise ImportError(_MISSING_REDIS_EXTRA_MESSAGE) from None
    return redis
