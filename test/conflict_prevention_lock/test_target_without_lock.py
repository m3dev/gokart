"""Dumping, loading and removing targets take no lock, even when a Redis lock backend is configured. https://github.com/m3dev/gokart/issues/713"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import patch

import fakeredis
import luigi
import luigi.configuration
import pytest
from redis.lock import Lock

import gokart


@pytest.fixture(autouse=True)
def redis_lock_url() -> Iterator[None]:
    config = luigi.configuration.get_config()
    existed = config.has_section('gokart_lock')
    if not existed:
        config.add_section('gokart_lock')
    previous = cast(str | None, config.get('gokart_lock', 'url', fallback=None))
    config.set('gokart_lock', 'url', 'redis://localhost:6379')
    # gokart_lock() is cached by luigi; conftest's _clear_luigi_instance_cache keeps it empty until the test body builds it
    yield
    # restore, so that a section from the developer's luigi.cfg survives the test
    if not existed:
        config.remove_section('gokart_lock')
    elif previous is None:
        config.remove_option('gokart_lock', 'url')
    else:
        config.set('gokart_lock', 'url', previous)


@pytest.fixture(autouse=True)
def fake_redis() -> Iterator[None]:
    server = fakeredis.FakeServer()
    with patch('redis.Redis', side_effect=lambda **kwargs: fakeredis.FakeRedis(server=server)):
        yield


class _Upstream(gokart.TaskOnKart[int]):
    task_namespace = __name__
    value: luigi.IntParameter = luigi.IntParameter(significant=False)

    def run(self):
        self.dump(self.value)


class _Downstream(gokart.TaskOnKart[int]):
    task_namespace = __name__
    value: luigi.IntParameter = luigi.IntParameter(significant=False)

    def requires(self):
        return _Upstream(value=self.value, workspace_directory=self.workspace_directory)

    def run(self):
        self.dump(self.load() * 10)


def test_dump_load_and_remove_take_no_lock(tmp_path: Path) -> None:
    target = _Upstream(value=1, workspace_directory=str(tmp_path)).make_target('output.pkl')

    with patch.object(Lock, 'acquire', autospec=True) as acquire:
        target.dump(1)
        assert target.load() == 1
        target.remove()

    acquire.assert_not_called()
    assert not target.exists()


def test_dump_overwrites_an_existing_output(tmp_path: Path) -> None:
    target = _Upstream(value=1, workspace_directory=str(tmp_path)).make_target('output.pkl')

    target.dump(1)
    target.dump(2)

    assert target.load() == 2


def test_modification_time_check_replaces_a_stale_output(tmp_path: Path) -> None:
    def downstream(value: int) -> _Downstream:
        return _Downstream(value=value, workspace_directory=str(tmp_path), modification_time_check=True)

    assert gokart.build(downstream(1), reset_register=False) == 10
    downstream_output: Any = downstream(1).output()
    # Make the upstream output newer than the downstream output without sleeping.
    upstream_output: Any = _Upstream(value=2, workspace_directory=str(tmp_path)).output()
    upstream_output.dump(2)
    os.utime(downstream_output.path(), (0, 0))

    assert gokart.build(downstream(2), reset_register=False) == 20
    assert downstream(2).complete()
