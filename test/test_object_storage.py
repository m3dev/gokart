import pytest
from luigi.format import Nop

from gokart.object_storage import ObjectStorage


@pytest.mark.parametrize(
    'call',
    [
        lambda path: ObjectStorage.get_object_storage_target(path, Nop),
        ObjectStorage.exists,
        ObjectStorage.get_timestamp,
        lambda path: ObjectStorage.get_zip_client(path, '/tmp'),
    ],
    ids=['get_object_storage_target', 'exists', 'get_timestamp', 'get_zip_client'],
)
def test_raises_value_error_for_non_object_storage_path(call):
    with pytest.raises(ValueError, match='local/file.pkl is not an object storage path'):
        call('local/file.pkl')
