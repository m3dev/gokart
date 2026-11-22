from __future__ import annotations

import warnings
from typing import NamedTuple
from urllib.parse import urlsplit

import luigi

DEFAULT_REDIS_PORT = 6379
DEFAULT_LOCK_TIMEOUT = 180


class gokart_lock(luigi.Config):
    """Configuration of where gokart stores its locks.

    You can set these options of section [gokart_lock] in your luigi.cfg file.

    NOTE: use snake_case for this class to match the luigi.Config convention.
    """

    url: luigi.OptionalParameter = luigi.OptionalParameter(
        default=None, description='URL of the lock backend, e.g. redis://localhost:6379. Task lock is deactivated, when None.'
    )
    timeout: luigi.IntParameter = luigi.IntParameter(default=DEFAULT_LOCK_TIMEOUT, description='A lock is released after `timeout` seconds without extension.')


class RedisLockSettings(NamedTuple):
    host: str | None
    port: int | None
    timeout: int


def parse_lock_url(url: str) -> tuple[str, int]:
    """Split a ``redis://host[:port]`` URL into (host, port)."""
    parts = urlsplit(url)
    # db and credentials are not supported yet; failing loudly beats silently connecting to the wrong database.
    if parts.scheme != 'redis' or not parts.hostname or parts.path not in ('', '/') or parts.username or parts.password or parts.query:
        raise ValueError(f'Unsupported lock backend URL: {url!r}. Only redis://host[:port] is supported.')
    return parts.hostname, parts.port or DEFAULT_REDIS_PORT


def resolve_redis_lock_settings(redis_host: str | None, redis_port: int | None, redis_timeout: int | None, warn: bool = True) -> RedisLockSettings:
    """Merge the deprecated task-level ``redis_*`` parameters with ``[gokart_lock]``; explicit task values win."""
    config = gokart_lock()
    config_host, config_port = parse_lock_url(config.url) if config.url else (None, None)
    explicit = [name for name, value in (('redis_host', redis_host), ('redis_port', redis_port), ('redis_timeout', redis_timeout)) if value is not None]
    if explicit and warn:
        warnings.warn(
            f'{", ".join(explicit)} of TaskOnKart is deprecated and will be removed in the next major release. Use `url` and `timeout` of the [gokart_lock] section instead.',
            DeprecationWarning,
            stacklevel=2,
        )
    return RedisLockSettings(
        host=redis_host if redis_host is not None else config_host,
        port=redis_port if redis_port is not None else config_port,
        timeout=redis_timeout if redis_timeout is not None else config.timeout,
    )
