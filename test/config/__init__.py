from pathlib import Path
from typing import Final

CONFIG_DIR: Final[Path] = Path(__file__).parent.resolve()
PYPROJECT_TOML: Final[Path] = CONFIG_DIR / 'pyproject.toml'
PYPROJECT_TOML_SET_DISALLOW_MISSING_PARAMETERS: Final[Path] = CONFIG_DIR / 'pyproject_disallow_missing_parameters.toml'
TEST_CONFIG_INI: Final[Path] = CONFIG_DIR / 'test_config.ini'
