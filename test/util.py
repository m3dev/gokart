import os
import uuid

import luigi


# TODO: use pytest.fixture to share this functionality with other tests
def _get_temporary_directory():
    _uuid = str(uuid.uuid4())
    return os.path.abspath(os.path.join(os.path.dirname(__name__), f'temporary-{_uuid}'))


class PlainLuigiTask(luigi.Task):
    """A plain luigi.Task (not a TaskOnKart) that succeeds by writing to `path`."""

    task_namespace = __name__
    path: luigi.Parameter[str] = luigi.Parameter()

    def output(self):  # pyright: ignore[reportIncompatibleMethodOverride]
        return luigi.LocalTarget(self.path)

    def run(self):
        with self.output().open('w') as f:
            f.write('ok')
