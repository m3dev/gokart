from __future__ import annotations

import os
import re
from typing import Any, NamedTuple, cast
from urllib.parse import urlsplit

import luigi

DEFAULT_REDIS_PORT = 6379
DEFAULT_LOCK_TIMEOUT = 180
REDIS_PASSWORD_ENV = 'GOKART_REDIS_PASSWORD'
DEFAULT_SKEW_MARGIN_SECONDS = 30.0
# Interval at which the lock holder extends its locks.
LOCK_EXTEND_SECONDS = 10
# The heartbeat extends every held lock one after another, so one slow request must not delay the others for long.
LOCK_REQUEST_TIMEOUT_SECONDS = 10
LOCK_REQUEST_RETRY_DEADLINE_SECONDS = 30
OBJECT_STORAGE_SCHEMES = ('gs', 's3')


class gokart_lock(luigi.Config):
    """Configuration of where gokart stores its locks.

    You can set these options of section [gokart_lock] in your luigi.cfg file.

    NOTE: use snake_case for this class to match the luigi.Config convention.
    """

    url: luigi.OptionalParameter = luigi.OptionalParameter(
        default=None, description='URL of the lock backend, e.g. redis://localhost:6379. Task lock is deactivated, when None.'
    )
    timeout: luigi.IntParameter = luigi.IntParameter(default=DEFAULT_LOCK_TIMEOUT, description='A lock is released after `timeout` seconds without extension.')
    skew_margin: luigi.FloatParameter = luigi.FloatParameter(
        default=DEFAULT_SKEW_MARGIN_SECONDS,
        description='Object storage backends only: extra seconds to wait after the server-side expiry of a lock, to tolerate clock skew between workers.',
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
    # A password in the URL would end up in the config file; it is read from the environment variable GOKART_REDIS_PASSWORD instead.
    if '@' in parts.netloc:
        raise ValueError(
            f'Unsupported lock backend URL: {url!r}. Do not put credentials in the URL; set the environment variable {REDIS_PASSWORD_ENV} instead.'
        )
    # urllib raises its own ValueError for a malformed port, which would not mention that the value comes from the lock URL.
    try:
        port = parts.port
    except ValueError:
        raise error from None
    # [0-9] rather than \d, which also matches non-ASCII digits.
    db_match = re.fullmatch(r'/?([0-9]*)', parts.path)
    if db_match is None:
        raise error
    return RedisUrl(parts.hostname, DEFAULT_REDIS_PORT if port is None else port, int(db_match.group(1) or 0), parts.scheme == 'rediss')


def redis_connection_kwargs(host: str | None, port: int | None, db: int, ssl: bool) -> dict[str, Any]:
    """Keyword arguments for ``redis.Redis``; options at their default are left out."""
    kwargs: dict[str, Any] = {'host': host or 'localhost', 'port': DEFAULT_REDIS_PORT if port is None else port}
    if db:
        kwargs['db'] = db
    if ssl:
        kwargs['ssl'] = True
    # An unset variable simply means that the server has no password.
    password = os.environ.get(REDIS_PASSWORD_ENV)
    if password:
        kwargs['password'] = password
    return kwargs


def parse_object_storage_lock_url(url: str, scheme: str) -> tuple[str, str]:
    """Split a ``<scheme>://bucket[/prefix]`` URL into (bucket, prefix)."""
    parts = urlsplit(url)
    # luigi's config parser keeps an inline `; comment` as part of the value, which would silently become part of the prefix.
    if parts.scheme != scheme or not parts.netloc or parts.query or parts.fragment or any(c.isspace() for c in url):
        raise ValueError(f'Unsupported lock backend URL: {url!r}. Expected {scheme}://bucket[/prefix] without whitespace.')
    return parts.netloc, parts.path.strip('/')


def lock_url_scheme(url: str | None) -> str | None:
    return urlsplit(url).scheme if url else None


def uses_object_storage_lock() -> bool:
    return lock_url_scheme(gokart_lock().url) in OBJECT_STORAGE_SCHEMES


def check_redis_lock_settings(timeout: int) -> None:
    """Reject a timeout under which the lock would expire between two heartbeats."""
    if timeout <= LOCK_EXTEND_SECONDS:
        raise ValueError(
            f'The lock timeout (`[gokart_lock] timeout`) must be greater than the heartbeat interval of {LOCK_EXTEND_SECONDS} seconds, not {timeout}.'
        )


def check_object_storage_lock_settings(timeout: int) -> None:
    """Reject settings under which a live lock would be taken over between two heartbeats."""
    # A heartbeat can take up to one request timeout plus the retry deadline before the next extend is attempted.
    minimum = LOCK_EXTEND_SECONDS + LOCK_REQUEST_TIMEOUT_SECONDS + LOCK_REQUEST_RETRY_DEADLINE_SECONDS
    if timeout <= minimum:
        raise ValueError(
            f'The lock timeout (`[gokart_lock] timeout`) must be greater than the heartbeat interval of {LOCK_EXTEND_SECONDS} seconds '
            f'plus the request timeout and retry deadline, {minimum} seconds in total, not {timeout}.'
        )
    skew_margin = gokart_lock().skew_margin
    if skew_margin < 0:
        raise ValueError(f'`[gokart_lock] skew_margin` must not be negative, not {skew_margin}.')


def resolve_redis_lock_settings() -> RedisLockSettings:
    """Resolve the Redis lock settings from ``[gokart_lock]``."""
    config = gokart_lock()
    scheme = lock_url_scheme(config.url)
    if scheme in OBJECT_STORAGE_SCHEMES:
        parse_object_storage_lock_url(cast(str, config.url), cast(str, scheme))
    elif scheme not in ('redis', 'rediss', None):
        raise ValueError(
            f'Unsupported lock backend URL: {config.url!r}. Only redis[s]://host[:port][/db], gs://bucket[/prefix] and s3://bucket[/prefix] are supported.'
        )
    if scheme in ('redis', 'rediss'):
        redis_url = parse_lock_url(cast(str, config.url))
        return RedisLockSettings(host=redis_url.host, port=redis_url.port, timeout=config.timeout, db=redis_url.db, ssl=redis_url.ssl)
    return RedisLockSettings(host=None, port=None, timeout=config.timeout)
