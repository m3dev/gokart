import functools
from pathlib import Path
from typing import Any
from unittest.mock import patch

import luigi
import pytest

from gokart import ListTaskInstanceParameter, TaskOnKart
from gokart.target import SingleFileTarget


class _Node(TaskOnKart[str]):
    task_namespace = __name__
    name: luigi.StrParameter = luigi.StrParameter()
    children: ListTaskInstanceParameter[TaskOnKart[Any]] = ListTaskInstanceParameter(default=[])

    def run(self):
        self.dump(self.name)


def _node(
    workspace: Path, name: str, children: list[TaskOnKart[Any]], strict_check: bool = True, modification_time_check: bool = False, rerun: bool = False
) -> _Node:
    task = _Node(
        name=name,
        children=children,
        workspace_directory=str(workspace),
        strict_check=strict_check,
        modification_time_check=modification_time_check,
        rerun=rerun,
    )
    task.dump(name)
    return task


@pytest.mark.parametrize('strict_check,modification_time_check', [(True, False), (False, True)])
def test_shared_dependency_is_checked_once_per_complete(tmp_path: Path, strict_check: bool, modification_time_check: bool) -> None:
    leaf = _node(tmp_path, 'leaf', [], strict_check, modification_time_check)
    top = _node(
        tmp_path,
        'top',
        [_node(tmp_path, 'left', [leaf], strict_check, modification_time_check), _node(tmp_path, 'right', [leaf], strict_check, modification_time_check)],
        strict_check,
        modification_time_check,
    )
    leaf_path = leaf.make_target().path()
    seen: list[str] = []
    original = SingleFileTarget.exists

    def spy(self: SingleFileTarget) -> bool:
        seen.append(self.path())
        return original(self)

    with patch.object(SingleFileTarget, 'exists', spy):
        assert top.complete()

    # once as the leaf's own output, once as the input of each of its two dependents.
    assert seen.count(leaf_path) == 3


def test_memo_does_not_leak_between_complete_calls(tmp_path: Path) -> None:
    leaf = _node(tmp_path, 'leaf', [])
    top = _node(tmp_path, 'top', [_node(tmp_path, 'left', [leaf]), _node(tmp_path, 'right', [leaf])])
    assert top.complete()

    leaf.make_target().remove()

    assert not top.complete()


def test_instances_with_same_task_id_but_different_rerun_are_checked_separately(tmp_path: Path) -> None:
    leaf = _node(tmp_path, 'leaf', [])
    rerun_leaf = _node(tmp_path, 'leaf', [], rerun=True)
    assert leaf.task_id == rerun_leaf.task_id and leaf is not rerun_leaf
    top = _node(tmp_path, 'top', [_node(tmp_path, 'left', [leaf]), _node(tmp_path, 'right', [rerun_leaf])])

    assert not top.complete()
    assert not rerun_leaf._rerun_state


def test_instances_with_same_task_id_but_different_strict_check_are_checked_separately(tmp_path: Path) -> None:
    missing_input = _node(tmp_path, 'missing', [])
    missing_input.make_target().remove()
    lenient = _node(tmp_path, 'leaf', [missing_input], strict_check=False)
    strict = _node(tmp_path, 'leaf', [missing_input], strict_check=True)
    assert lenient.task_id == strict.task_id and lenient is not strict
    top = _node(tmp_path, 'top', [_node(tmp_path, 'left', [lenient]), _node(tmp_path, 'right', [strict])])

    assert not top.complete()


def test_instance_level_complete_override_of_requirement_is_respected(tmp_path: Path) -> None:
    leaf = _node(tmp_path, 'leaf', [])
    top = _node(tmp_path, 'top', [leaf])

    with patch.object(leaf, 'complete', return_value=False):
        assert not top.complete()


def test_class_level_complete_wrapper_is_applied_to_requirements(tmp_path: Path) -> None:
    leaf = _node(tmp_path, 'leaf', [])
    top = _node(tmp_path, 'top', [leaf])
    original = TaskOnKart.complete
    seen: list[str] = []

    @functools.wraps(original)
    def traced(self: _Node) -> bool:
        seen.append(self.name)
        return original(self)

    with patch.object(TaskOnKart, 'complete', traced):
        assert top.complete()

    assert seen == ['top', 'leaf']


class _Chain(TaskOnKart[int]):
    task_namespace = __name__
    index: luigi.IntParameter = luigi.IntParameter()

    def requires(self):
        return _Chain(index=self.index - 1, workspace_directory=self.workspace_directory, strict_check=True) if self.index else []

    def run(self):
        self.dump(self.index)


def test_complete_handles_a_long_strict_check_chain(tmp_path: Path) -> None:
    # Each level costs two Python frames, so a chain this long must stay within the default recursion limit.
    length = 300
    for index in range(length):
        _Chain(index=index, workspace_directory=str(tmp_path), strict_check=True).dump(index)

    assert _Chain(index=length - 1, workspace_directory=str(tmp_path), strict_check=True).complete()
