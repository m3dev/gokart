from __future__ import annotations

from types import ModuleType

from gokart._optional_dependency import import_optional_dependency


def import_redis() -> ModuleType:
    return import_optional_dependency('redis', extra='redis', feature='Redis lock')
