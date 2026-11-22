import os
from unittest.mock import patch

import pytest

from gokart.process_cache import cached_per_process


def test_creates_value_when_nothing_is_cached():
    value = object()

    assert cached_per_process(None, lambda: value) == (os.getpid(), value)


def test_reuses_value_created_in_this_process():
    cached = (os.getpid(), object())

    assert cached_per_process(cached, lambda: pytest.fail('must not create a new value')) is cached


def test_recreates_value_in_a_forked_process():
    cached = (os.getpid(), object())
    value = object()
    child_pid = os.getpid() + 1

    with patch('os.getpid', return_value=child_pid):
        assert cached_per_process(cached, lambda: value) == (child_pid, value)
