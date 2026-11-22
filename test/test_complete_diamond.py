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


def _build_diamond(workspace: Path, strict_check: bool, modification_time_check: bool, leaf_rerun: bool = False) -> tuple[_Node, _Node]:
    def node(name: str, children: list[TaskOnKart[Any]], rerun: bool = False) -> _Node:
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

    leaf = node('leaf', [], rerun=leaf_rerun)
    top = node('top', [node('left', [leaf]), node('right', [leaf])])
    return top, leaf


@pytest.mark.parametrize('strict_check,modification_time_check', [(True, False), (False, True)])
def test_shared_dependency_is_checked_once_per_complete(tmp_path: Path, strict_check: bool, modification_time_check: bool) -> None:
    top, leaf = _build_diamond(tmp_path, strict_check, modification_time_check)
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
    top, leaf = _build_diamond(tmp_path, strict_check=True, modification_time_check=False)
    assert top.complete()

    leaf.make_target().remove()

    assert not top.complete()


def test_rerun_state_of_shared_dependency_is_consumed_once(tmp_path: Path) -> None:
    top, leaf = _build_diamond(tmp_path, strict_check=True, modification_time_check=False, leaf_rerun=True)

    assert not top.complete()
    assert not leaf._rerun_state
