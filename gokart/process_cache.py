from __future__ import annotations

import os
from collections.abc import Callable
from typing import TypeVar

T = TypeVar('T')


def cached_per_process(cached: tuple[int, T] | None, create: Callable[[], T]) -> tuple[int, T]:
    """Return ``cached`` if it was created in this process, otherwise a new ``(pid, value)`` from ``create``.

    Use it for objects that are not fork-safe, so that each process (e.g. worker_processes > 1) builds its own instead of reusing the parent's.
    """
    # No lock on purpose: threads racing on the first call may each create a value, which is harmless since the last write wins.
    pid = os.getpid()
    if cached is None or cached[0] != pid:
        return (pid, create())
    return cached
