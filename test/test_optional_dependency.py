from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from gokart._optional_dependency import import_optional_dependency


@pytest.fixture
def fake_extra(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[Path]:
    package = tmp_path / '_gokart_fake_extra'
    (package / 'sub').mkdir(parents=True)
    (package / '__init__.py').write_text('')
    (package / 'sub' / '__init__.py').write_text('')
    monkeypatch.syspath_prepend(str(tmp_path))
    yield package
    for name in [name for name in sys.modules if name.split('.')[0] == '_gokart_fake_extra']:
        del sys.modules[name]


def test_returns_module(fake_extra: Path) -> None:
    assert import_optional_dependency('_gokart_fake_extra.sub', extra='fake', feature='Fake').__name__ == '_gokart_fake_extra.sub'


@pytest.mark.parametrize(
    ('module_name', 'missing'),
    [
        pytest.param('_gokart_not_installed', '_gokart_not_installed', id='module'),
        pytest.param('_gokart_not_installed.sub', '_gokart_not_installed', id='parent-package'),
    ],
)
def test_raises_install_hint_when_module_or_its_parent_is_missing(module_name: str, missing: str) -> None:
    with pytest.raises(ImportError, match=r'^Fake support requires additional dependencies\. Install them with: pip install gokart\[fake\]$') as exc_info:
        import_optional_dependency(module_name, extra='fake', feature='Fake')
    # Keeping the cause helps when the hint is not the whole story.
    assert isinstance(exc_info.value.__cause__, ModuleNotFoundError)
    assert exc_info.value.__cause__.name == missing


@pytest.mark.parametrize(
    ('init', 'expected_error', 'expected_message'),
    [
        pytest.param('import _gokart_missing_dependency', ModuleNotFoundError, '_gokart_missing_dependency', id='missing-dependency'),
        pytest.param(
            'import _gokart_fake_extra.sub._missing_submodule', ModuleNotFoundError, r'_gokart_fake_extra\.sub\._missing_submodule', id='missing-submodule'
        ),
        pytest.param("raise ImportError('broken dependency')", ImportError, 'broken dependency', id='import-error'),
    ],
)
def test_keeps_original_error_when_installed_module_fails_to_import(
    fake_extra: Path, init: str, expected_error: type[ImportError], expected_message: str
) -> None:
    # The install hint would be wrong here and would hide the real cause.
    (fake_extra / 'sub' / '__init__.py').write_text(init)
    with pytest.raises(expected_error, match=expected_message) as exc_info:
        import_optional_dependency('_gokart_fake_extra.sub', extra='fake', feature='Fake')
    assert 'gokart[fake]' not in str(exc_info.value)
