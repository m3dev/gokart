"""gokart warns about deprecations with FutureWarning, not DeprecationWarning or PendingDeprecationWarning.

Python hides DeprecationWarning (and PendingDeprecationWarning) by default unless it is raised from __main__,
and tasks are usually defined in other modules, so users would not see the warning before the API goes away.
Deprecations added with DeprecationWarning had to be switched to FutureWarning by hand three times
(#646, #680, #689), so this test catches the next one.
"""

from __future__ import annotations

import ast
import textwrap
from pathlib import Path

import pytest

_GOKART_DIR = Path(__file__).resolve().parent.parent / 'gokart'
_HIDDEN_CATEGORIES = frozenset({'DeprecationWarning', 'PendingDeprecationWarning'})
# warnings.deprecated (3.13+) and typing_extensions.deprecated warn with DeprecationWarning unless category= says otherwise.
_DEPRECATED_MODULES = frozenset({'warnings', 'typing_extensions'})

# (path relative to gokart/, qualified name of the enclosing function) -> why it may use a hidden category
_ALLOWED: dict[tuple[str, str], str] = {}


def _name(node: ast.expr) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    return None


def _hidden_categories(trees: list[ast.Module]) -> frozenset[str]:
    """Return the hidden categories plus the names of every class in trees that subclasses one, directly or not."""
    classes = [node for tree in trees for node in ast.walk(tree) if isinstance(node, ast.ClassDef)]
    hidden = set(_HIDDEN_CATEGORIES)
    while new := {c.name for c in classes if c.name not in hidden and any(_name(base) in hidden for base in c.bases)}:
        hidden |= new
    return frozenset(hidden)


def _is_hidden(category: ast.expr, hidden: frozenset[str]) -> bool:
    # warnings.warn(DeprecationWarning('x')) takes the category from the instance.
    if isinstance(category, ast.Call):
        category = category.func
    return _name(category) in hidden


def _find_hidden_warnings(tree: ast.Module, hidden: frozenset[str] = _HIDDEN_CATEGORIES) -> list[tuple[str, int]]:
    """Return (qualified name of the enclosing function, line) of each warning emitted with a hidden category."""
    module_aliases: dict[str, set[str]] = {module: set() for module in _DEPRECATED_MODULES}
    function_aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name in module_aliases:
                    module_aliases[alias.name].add(alias.asname or alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module in module_aliases:
            for alias in node.names:
                if (node.module, alias.name) in {('warnings', 'warn'), ('warnings', 'warn_explicit')} or alias.name == 'deprecated':
                    function_aliases[alias.asname or alias.name] = alias.name

    def function_of(func: ast.expr) -> str | None:
        if isinstance(func, ast.Name):
            return function_aliases.get(func.id)
        if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
            if func.value.id in module_aliases['warnings'] and func.attr in {'warn', 'warn_explicit', 'deprecated'}:
                return func.attr
            if func.value.id in module_aliases['typing_extensions'] and func.attr == 'deprecated':
                return func.attr
        return None

    def emits_hidden(call: ast.Call, function: str) -> bool:
        keyword_categories = [keyword.value for keyword in call.keywords if keyword.arg == 'category']
        if function == 'deprecated':
            return not keyword_categories or _is_hidden(keyword_categories[0], hidden)
        return any(_is_hidden(category, hidden) for category in call.args[:2] + keyword_categories)

    found: list[tuple[str, int]] = []

    def visit(node: ast.AST, scope: list[str]) -> None:
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            # Decorators run in the enclosing scope, at definition time.
            for decorator in node.decorator_list:
                visit(decorator, scope)
            scope = [*scope, node.name]
        if isinstance(node, ast.Call) and (function := function_of(node.func)) is not None and emits_hidden(node, function):
            found.append(('.'.join(scope) or '<module>', node.lineno))
        for child in ast.iter_child_nodes(node):
            if not (isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef) and child in node.decorator_list):
                visit(child, scope)

    visit(tree, [])
    return found


def test_gokart_does_not_warn_with_hidden_categories() -> None:
    trees = {
        path.relative_to(_GOKART_DIR).as_posix(): ast.parse(path.read_text(encoding='utf-8'), filename=str(path)) for path in sorted(_GOKART_DIR.rglob('*.py'))
    }
    hidden = _hidden_categories(list(trees.values()))
    found = [(path, scope, line) for path, tree in trees.items() for scope, line in _find_hidden_warnings(tree, hidden)]

    not_allowed = [f'gokart/{path}:{line} in {scope}' for path, scope, line in found if (path, scope) not in _ALLOWED]
    assert not_allowed == [], 'Use FutureWarning for deprecations; DeprecationWarning is hidden by default outside __main__.'
    stale = sorted(set(_ALLOWED) - {(path, scope) for path, scope, _ in found})
    assert stale == [], 'Remove the entries of _ALLOWED that no longer warn with a hidden category.'


@pytest.mark.parametrize(
    'source',
    [
        "import warnings\nwarnings.warn('x', DeprecationWarning)",
        "import warnings\nwarnings.warn('x', category=PendingDeprecationWarning, stacklevel=2)",
        "import builtins, warnings\nwarnings.warn('x', builtins.DeprecationWarning)",
        "from warnings import warn\nwarn('x', DeprecationWarning)",
        "from warnings import warn as w\nw('x', category=DeprecationWarning)",
        "import warnings as w\nw.warn('x', DeprecationWarning)",
        "import warnings\nwarnings.warn(DeprecationWarning('x'))",
        "import warnings\nwarnings.warn_explicit('x', DeprecationWarning, 'f.py', 1)",
        "from warnings import deprecated\n@deprecated('x')\ndef f(): ...",
        "import typing_extensions\n@typing_extensions.deprecated('x', category=PendingDeprecationWarning)\ndef f(): ...",
    ],
)
def test_find_hidden_warnings_detects(source: str) -> None:
    assert _find_hidden_warnings(ast.parse(source)) == [('<module>', 2)]


def test_find_hidden_warnings_detects_subclasses_of_hidden_categories() -> None:
    source = "import warnings\nclass A(DeprecationWarning): ...\nclass B(A): ...\nwarnings.warn('x', B)"
    tree = ast.parse(source)

    assert _find_hidden_warnings(tree, _hidden_categories([tree])) == [('<module>', 4)]


@pytest.mark.parametrize(
    'source',
    [
        "import warnings\nwarnings.warn('x', FutureWarning)",
        "import warnings\nwarnings.warn('x')",
        "import warnings\nwarnings.warn('DeprecationWarning', stacklevel=2)",
        "def warn(message, category): ...\nwarn('x', DeprecationWarning)",
        "from warnings import deprecated\n@deprecated('x', category=FutureWarning)\ndef f(): ...",
        "from warnings import deprecated\n@deprecated('x', category=None)\ndef f(): ...",
    ],
)
def test_find_hidden_warnings_ignores(source: str) -> None:
    assert _find_hidden_warnings(ast.parse(source)) == []


def test_find_hidden_warnings_reports_the_enclosing_function() -> None:
    source = textwrap.dedent(
        """
        import warnings

        class A:
            def f(self):
                warnings.warn('x', DeprecationWarning)
        """
    )

    assert _find_hidden_warnings(ast.parse(source)) == [('A.f', 6)]
