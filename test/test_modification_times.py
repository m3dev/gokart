import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import cast
from unittest.mock import Mock, patch

import boto3
import pytest
from moto import mock_aws

from gokart.conflict_prevention_lock.task_lock import TaskLockParams
from gokart.in_memory import make_in_memory_target
from gokart.in_memory.repository import acquire_repository, in_memory_scope
from gokart.target import TargetOnKart, make_target
from gokart.task import TaskOnKart


@pytest.fixture(params=['UTC0', 'JST-9'])
def local_timezone(request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    if not hasattr(time, 'tzset'):
        pytest.skip('changing the process timezone requires time.tzset')
    try:
        with monkeypatch.context() as context:
            context.setenv('TZ', request.param)
            time.tzset()
            yield
    finally:
        time.tzset()


@pytest.mark.parametrize('delta', [-60, 0, 60])
def test_local_and_s3_modification_times(local_timezone, tmp_path: Path, delta: int) -> None:
    with mock_aws():
        boto3.client('s3', region_name='us-east-1').create_bucket(Bucket='test-modification-times')
        local_path = tmp_path / 'input.pkl'
        local = make_target(str(local_path))
        remote = make_target('s3://test-modification-times/output.pkl')
        local.dump('input')
        remote.dump('output')
        timestamp = remote.last_modification_time().timestamp() + delta
        os.utime(local_path, (timestamp, timestamp))

        assert local.last_modification_time().tzinfo is None
        assert remote.last_modification_time().utcoffset() is not None
        assert TaskOnKart._check_modification_time([local], [remote]) is (delta <= 0)
        assert TaskOnKart._check_modification_time([remote], [local]) is (delta >= 0)


@pytest.mark.parametrize('delta', [-60, 0, 60])
def test_local_and_gcs_modification_times(local_timezone, tmp_path: Path, gcs_bucket: str, delta: int) -> None:
    local_path = tmp_path / 'input.pkl'
    local = make_target(str(local_path))
    remote = make_target(f'gs://{gcs_bucket}/output.pkl')
    local.dump('input')
    remote.dump('output')
    timestamp = remote.last_modification_time().timestamp() + delta
    os.utime(local_path, (timestamp, timestamp))

    assert local.last_modification_time().tzinfo is None
    assert remote.last_modification_time().utcoffset() is not None
    assert TaskOnKart._check_modification_time([local], [remote]) is (delta <= 0)
    assert TaskOnKart._check_modification_time([remote], [local]) is (delta >= 0)


def _target(path: str, modified: datetime) -> Mock:
    target = Mock(spec=TargetOnKart)
    target.path.return_value = path
    target.last_modification_time.return_value = modified
    return target


@pytest.mark.parametrize('last_input', [2, 4])
def test_mixed_timestamps_are_normalized_before_max_and_min(local_timezone, last_input: int) -> None:
    base = datetime(2020, 1, 1, tzinfo=UTC)
    inputs = [
        _target('local-input', datetime.fromtimestamp(base.timestamp() + 1)),
        _target('remote-input', base + timedelta(seconds=last_input)),
    ]
    outputs = [
        _target('local-output', datetime.fromtimestamp(base.timestamp() + 5)),
        _target('remote-output', base + timedelta(seconds=3)),
    ]
    assert TaskOnKart._check_modification_time(cast(list[TargetOnKart], inputs), cast(list[TargetOnKart], outputs)) is (last_input <= 3)
    for target in inputs + outputs:
        target.last_modification_time.assert_called_once_with()


def test_custom_naive_and_non_utc_aware_timestamps(local_timezone) -> None:
    instant = datetime(2020, 1, 1, tzinfo=UTC)
    local = _target('custom-naive', datetime.fromtimestamp(instant.timestamp()))
    remote = _target('custom-aware', instant.astimezone(timezone(timedelta(hours=-7))))
    assert TaskOnKart._check_modification_time([local], [remote])
    assert TaskOnKart._check_modification_time([remote], [local])


def test_in_memory_and_aware_timestamps(local_timezone) -> None:
    instant = datetime(2020, 1, 1, tzinfo=UTC)
    lock = TaskLockParams(None, None, None, 'timestamp-test', False, False, 0)
    memory = make_in_memory_target('timestamp-test', lock)
    remote = _target('remote', instant)
    # in-memory targets are stored only inside gokart.build; the scope stands in for it and drops the store on exit
    with in_memory_scope():
        acquire_repository()
        with patch('gokart.in_memory.data.datetime') as clock:
            clock.now.return_value = datetime.fromtimestamp(instant.timestamp())
            memory.dump('input')
        assert memory.last_modification_time().tzinfo is None
        assert TaskOnKart._check_modification_time([memory], [remote])
        assert TaskOnKart._check_modification_time([remote], [memory])


@pytest.mark.parametrize(('has_input', 'has_output'), [(False, False), (True, False), (False, True)])
def test_empty_target_lists(has_input: bool, has_output: bool) -> None:
    target = _target('target', datetime(2020, 1, 1, tzinfo=UTC))
    assert TaskOnKart._check_modification_time([target] if has_input else [], [target] if has_output else [])


def test_common_targets_do_not_read_timestamps() -> None:
    shared = Mock(spec=TargetOnKart)
    shared.path.return_value = 'shared'
    shared.last_modification_time.side_effect = AssertionError('common targets must be skipped')
    assert TaskOnKart._check_modification_time([shared], [shared])
    shared.last_modification_time.assert_not_called()


@pytest.mark.parametrize('tzinfo', [None, UTC])
def test_homogeneous_timestamps_at_calendar_boundaries(tzinfo) -> None:
    earlier = _target('earlier', datetime.min.replace(tzinfo=tzinfo))
    later = _target('later', datetime.max.replace(tzinfo=tzinfo))
    assert TaskOnKart._check_modification_time([earlier], [later])
    assert not TaskOnKart._check_modification_time([later], [earlier])
    assert TaskOnKart._check_modification_time([earlier], [_target('same-earlier', earlier.last_modification_time())])
    assert TaskOnKart._check_modification_time([later], [_target('same-later', later.last_modification_time())])


@pytest.mark.parametrize('local_timezone', ['EST5EDT,M3.2.0,M11.1.0'], indirect=True)
def test_mixed_timestamps_during_daylight_saving_fold(local_timezone) -> None:
    before_fold = datetime(2020, 11, 1, 5, 30, tzinfo=UTC)
    after_fold = before_fold + timedelta(hours=1)
    earlier = _target('earlier', datetime.fromtimestamp(before_fold.timestamp()))
    later = _target('later', datetime.fromtimestamp(after_fold.timestamp()))
    assert earlier.last_modification_time().hour == later.last_modification_time().hour == 1
    assert TaskOnKart._check_modification_time([earlier], [_target('remote-after', after_fold)])
    assert not TaskOnKart._check_modification_time([later], [_target('remote-before', before_fold)])


def test_in_memory_target_with_same_path_as_file_is_not_common(tmp_path: Path) -> None:
    # The key of an in-memory output is built like a file path, so it can equal the path of a file it does not share data with.
    path = str(tmp_path / 'same.pkl')
    file = make_target(path)
    memory = make_in_memory_target(path)
    with in_memory_scope():
        acquire_repository()
        with patch('gokart.in_memory.data.datetime') as clock:
            clock.now.return_value = datetime(2020, 1, 1)
            memory.dump('output')
        file.dump('newer input')
        assert not TaskOnKart._check_modification_time([file], [memory])
