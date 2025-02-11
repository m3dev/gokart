import os
import uuid


# TODO: use pytest.fixture to share this functionality with other tests
def _get_temporary_directory():
    _uuid = str(uuid.uuid4())
    return os.path.abspath(os.path.join(os.path.dirname(__name__), f'temporary-{_uuid}'))
