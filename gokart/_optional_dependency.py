from __future__ import annotations

import importlib
from types import ModuleType


def import_optional_dependency(module_name: str, *, extra: str, feature: str) -> ModuleType:
    """Import a module that comes with an optional extra of gokart.

    Only when the module itself is not installed is the error replaced with a hint to install the extra. Any other failure (a missing
    dependency of the module, a broken install) is re-raised as it is, since the hint would be wrong and hide the real cause.
    """
    try:
        return importlib.import_module(module_name)
    except ModuleNotFoundError as e:
        if not is_missing_module(e, module_name):
            raise
        raise ImportError(f'{feature} support requires additional dependencies. Install them with: pip install gokart[{extra}]') from None


def is_missing_module(error: ModuleNotFoundError, module_name: str) -> bool:
    # A missing parent package (e.g. google.cloud for google.cloud.storage) also means that the module is not installed.
    parts = module_name.split('.')
    return error.name in {'.'.join(parts[:i]) for i in range(1, len(parts) + 1)}
