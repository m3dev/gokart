import os
import uuid

import luigi


# TODO: use pytest.fixture to share this functionality with other tests
def _get_temporary_directory():
    _uuid = str(uuid.uuid4())
    return os.path.abspath(os.path.join(os.path.dirname(__name__), f'temporary-{_uuid}'))


class _PlainLuigiTarget(luigi.Target):
    """A target that is not TargetOnKart, which output() may return at runtime despite its annotation."""

    def __init__(self, path: str = 'plain', exists: bool = False) -> None:
        self._path = path
        self._exists = exists

    def exists(self) -> bool:  # pyright: ignore[reportIncompatibleMethodOverride]
        return self._exists

    def path(self) -> str:
        return self._path
