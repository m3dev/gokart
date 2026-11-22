from __future__ import annotations

import sys
from types import ModuleType
from typing import Any

from gokart._optional_dependency import import_optional_dependency


def import_pandas() -> ModuleType:
    return import_optional_dependency('pandas', extra='pandas', feature='pandas')


def is_pandas_data_frame(obj: Any) -> bool:
    # WHY: a DataFrame can only exist once pandas has been imported, so there is no need to import pandas here: that would load pandas
    # on every dump, even in pipelines that never use it.
    # getattr: while another thread is importing pandas, the module is in sys.modules before DataFrame is defined.
    data_frame = getattr(sys.modules.get('pandas'), 'DataFrame', None)
    return data_frame is not None and isinstance(obj, data_frame)
