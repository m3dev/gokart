from __future__ import annotations

import os
import re
import warnings
from typing import Any, NamedTuple
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
    password_env: luigi.OptionalParameter = luigi.OptionalParameter(
        default=None, description='Redis only: name of the environment variable that holds the password, so that it stays out of the config file.'
    )


class RedisLockSettings(NamedTuple):
    host: str | None
    port: int | None
    timeout: int
    db: int = 0
    ssl: bool = False


class RedisUrl(NamedTuple):
    host: str
    port: int
    db: int
    ssl: bool


def parse_lock_url(url: str) -> RedisUrl:
    """Split a ``redis[s]://host[:port][/db]`` URL into its parts."""
    parts = urlsplit(url)
    error = ValueError(f'Unsupported lock backend URL: {url!r}. Expected redis://host[:port][/db] or rediss://host[:port][/db].')
    if parts.scheme not in ('redis', 'rediss') or not parts.hostname or parts.query or parts.fragment:
        raise error
    # A password in the URL would end up in the config file; it is read from the environment variable named by `password_env` instead.
    if '@' in parts.netloc:
        raise ValueError(
            f'Unsupported lock backend URL: {url!r}. Do not put credentials in the URL; set `password_env` to the name of an environment variable instead.'
        )
    db_match = re.fullmatch(r'/?(\d*)', parts.path)
    if db_match is None:
        raise error
    return RedisUrl(parts.hostname, parts.port or DEFAULT_REDIS_PORT, int(db_match.group(1) or 0), parts.scheme == 'rediss')


def redis_connection_kwargs(host: str | None, port: int | None, db: int, ssl: bool) -> dict[str, Any]:
    """Keyword arguments for ``redis.Redis``; options at their default are left out."""
    kwargs: dict[str, Any] = {'host': host or 'localhost', 'port': port or DEFAULT_REDIS_PORT}
    if db:
        kwargs['db'] = db
    if ssl:
        kwargs['ssl'] = True
    password_env = gokart_lock().password_env
    if password_env:
        password = os.environ.get(password_env)
        if not password:
            raise ValueError(f'`password_env` of the [gokart_lock] section is {password_env!r}, but that environment variable is not set or empty.')
        kwargs['password'] = password
    return kwargs


def resolve_redis_lock_settings(redis_host: str | None, redis_port: int | None, redis_timeout: int | None, warn: bool = True) -> RedisLockSettings:
    """Merge the deprecated task-level ``redis_*`` parameters with ``[gokart_lock]``; explicit task values win."""
    config = gokart_lock()
    redis_url = parse_lock_url(config.url) if config.url else None
    explicit = [name for name, value in (('redis_host', redis_host), ('redis_port', redis_port), ('redis_timeout', redis_timeout)) if value is not None]
    if explicit and warn:
        warnings.warn(
            f'{", ".join(explicit)} of TaskOnKart is deprecated and will be removed in the next major release. Use `url` and `timeout` of the [gokart_lock] section instead.',
            DeprecationWarning,
            stacklevel=2,
        )
    # db and TLS describe the server that the URL points at, so they do not carry over to an explicit task-level host.
    url_describes_server = redis_url is not None and redis_host is None
    return RedisLockSettings(
        host=redis_host if redis_host is not None else (redis_url.host if redis_url else None),
        port=redis_port if redis_port is not None else (redis_url.port if redis_url else None),
        timeout=redis_timeout if redis_timeout is not None else config.timeout,
        db=redis_url.db if redis_url is not None and url_describes_server else 0,
        ssl=redis_url.ssl if redis_url is not None and url_describes_server else False,
    )
