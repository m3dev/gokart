import os
from collections.abc import Iterator
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolated_working_directory(tmp_path: Path) -> Iterator[None]:
    # TaskOnKart's default workspace_directory is './resources/', so tests that don't set it would leave files in the repository
    original = Path.cwd()
    os.chdir(tmp_path)
    try:
        yield
    finally:
        os.chdir(original)
