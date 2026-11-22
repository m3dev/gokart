from __future__ import annotations

from types import ModuleType

_MISSING_REDIS_EXTRA_MESSAGE = 'Redis lock support requires additional dependencies. Install them with: pip install gokart[redis]'


def import_redis() -> ModuleType:
    try:
        import redis
    except ModuleNotFoundError as e:
        # Only a missing redis itself means the extra is not installed. Other failures (a missing dependency of redis,
        # a broken redis install) are re-raised as they are, since the install hint would be wrong and hide the real cause.
        if e.name != 'redis':
            raise
        raise ImportError(_MISSING_REDIS_EXTRA_MESSAGE) from None
    return redis
