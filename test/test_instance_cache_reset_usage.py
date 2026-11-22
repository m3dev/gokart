import ast
from pathlib import Path

_TEST_DIR = Path(__file__).parent


def _per_class_instance_cache_resets(source: str) -> list[int]:
    lines = []
    for node in ast.walk(ast.parse(source)):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute) and node.func.attr == 'clear_instance_cache'):
            continue
        owner = node.func.value
        owner_name = owner.attr if isinstance(owner, ast.Attribute) else owner.id if isinstance(owner, ast.Name) else None
        if owner_name != 'Register':
            lines.append(node.lineno)
    return lines


def test_detects_a_reset_on_a_subclass():
    assert _per_class_instance_cache_resets('_DummyTask.clear_instance_cache()\nmodule.SomeConfig.clear_instance_cache()\n') == [1, 2]


def test_allows_the_reset_on_register():
    assert _per_class_instance_cache_resets('Register.clear_instance_cache()\nluigi.task_register.Register.clear_instance_cache()\n') == []


def test_tests_reset_the_instance_cache_only_through_register():
    # clear_instance_cache() is a classmethod of luigi's Register metaclass, so `_DummyTask.clear_instance_cache()` reads as a per-class reset
    # but actually resets the one cache shared by every Task and Config. test/conftest.py already does that around every test.
    violations = [
        f'{path.relative_to(_TEST_DIR)}:{line}' for path in sorted(_TEST_DIR.rglob('*.py')) for line in _per_class_instance_cache_resets(path.read_text())
    ]
    assert violations == [], 'Call Register.clear_instance_cache() instead, or rely on the autouse fixture in test/conftest.py'
