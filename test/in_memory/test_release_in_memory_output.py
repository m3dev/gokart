"""In-memory outputs are released once every task that requires them has finished."""

import functools
from typing import Any

import luigi
import pytest
from luigi.configuration import get_config

import gokart
from gokart.build import GokartBuildError
from gokart.in_memory.repository import get_current_repository

# Snapshot of the in-memory keys taken at the start of each task's run(), keyed by task name.
_stored_keys_at_run: dict[str, set[str]] = {}
_in_memory_task_runs: list[str] = []


def _stored_keys() -> set[str]:
    repository = get_current_repository()
    assert repository is not None
    return {key for key, _ in repository.get_gen()}


class _InMemoryTask(gokart.TaskOnKart[str]):
    task_namespace = __name__
    name: luigi.Parameter[str] = luigi.Parameter()
    upstream: gokart.ListTaskInstanceParameter[gokart.TaskOnKart[str]] = gokart.ListTaskInstanceParameter(default=[])
    fail: luigi.BoolParameter = luigi.BoolParameter(default=False)
    order: luigi.IntParameter = luigi.IntParameter(default=0, significant=False)

    @property
    def priority(self):
        # WHY: TaskOnKart randomizes priority; a fixed one makes the run order of runnable tasks deterministic.
        return self.order

    def output(self):
        return self.make_target(f'{self.name}.pkl', persist=False)

    def requires(self) -> Any:
        return self.upstream

    def run(self):
        _stored_keys_at_run[self.name] = _stored_keys()
        _in_memory_task_runs.append(self.name)
        if self.fail:
            raise RuntimeError('fail')
        self.dump(self.name + ''.join(self.load()))


_yield_task_runs: list[int] = []


class _YieldTask(gokart.TaskOnKart[str]):
    """Requires `static` up front and `dynamic` from run(), after `static` has finished."""

    task_namespace = __name__
    static: gokart.TaskInstanceParameter[gokart.TaskOnKart[str]] = gokart.TaskInstanceParameter()
    dynamic: gokart.TaskInstanceParameter[gokart.TaskOnKart[str]] = gokart.TaskInstanceParameter()

    def output(self):
        return self.make_target('yield.pkl', persist=False)

    def requires(self) -> Any:
        return self.static

    def run(self):  # pyright: ignore[reportIncompatibleMethodOverride]  # luigi supports dynamic dependencies by yielding from run()
        _yield_task_runs.append(1)
        dynamic = yield self.dynamic
        self.dump(self.load(self.static) + dynamic.load())


def _passthrough(func: Any) -> Any:
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    return wrapper


class _DecoratedYieldTask(_YieldTask):
    """run() returns a generator, but the decorator hides it from inspect.isgeneratorfunction."""

    @_passthrough
    def run(self):
        _yield_task_runs.append(1)
        dynamic = yield self.dynamic
        self.dump(self.load(self.static) + dynamic.load())


class _HelperYieldTask(_YieldTask):
    """run() returns a generator made by another method, so no check on run() itself can tell that it yields."""

    def run(self):
        return super().run()


class _PassThroughTask(gokart.TaskOnKart[str]):
    """Uses the output of the required task as its own output, so the two tasks share one in-memory key."""

    task_namespace = __name__
    upstream: gokart.TaskInstanceParameter[gokart.TaskOnKart[str]] = gokart.TaskInstanceParameter()

    def output(self):
        return self.input()

    def requires(self) -> Any:
        return self.upstream

    def run(self):
        pass


def _key(task: gokart.TaskOnKart[Any]) -> str:
    return task.output().path()  # type: ignore[union-attr]


@pytest.fixture(autouse=True)
def _clear_snapshots() -> None:
    _stored_keys_at_run.clear()
    _in_memory_task_runs.clear()


class TestReleaseInMemoryOutput:
    def test_output_is_released_after_all_dependents_finish(self) -> None:
        # a is required by b and c; d requires b and c. Tasks run in `order`: b, c, then d.
        a = _InMemoryTask(name='a')
        b = _InMemoryTask(name='b', upstream=[a], order=3)
        c = _InMemoryTask(name='c', upstream=[a], order=2)
        d = _InMemoryTask(name='d', upstream=[b, c])

        assert gokart.build(d, reset_register=False) == 'dbaca'

        assert _key(a) in _stored_keys_at_run['c']
        assert _stored_keys_at_run['d'] == {_key(b), _key(c)}

    def test_root_output_is_kept_for_return_value(self) -> None:
        a = _InMemoryTask(name='a')
        assert gokart.build(_InMemoryTask(name='b', upstream=[a]), reset_register=False) == 'ba'

    def test_output_shared_with_a_dependent_is_kept_while_the_dependent_needs_it(self) -> None:
        # The output of a is also the output of b, so releasing a after b finished must not remove what c loads from b.
        a = _InMemoryTask(name='a')
        b = _PassThroughTask(upstream=a)
        c = _InMemoryTask(name='c', upstream=[b])

        assert gokart.build(c, reset_register=False) == 'ca'
        assert _in_memory_task_runs == ['a', 'c']

    def test_output_is_kept_when_a_dependent_fails(self) -> None:
        a = _InMemoryTask(name='a')
        b = _InMemoryTask(name='b', upstream=[a], order=3)
        c = _InMemoryTask(name='c', upstream=[a], fail=True, order=2)
        e = _InMemoryTask(name='e', upstream=[b], order=1)
        d = _InMemoryTask(name='d', upstream=[c, e])

        with pytest.raises(GokartBuildError):
            gokart.build(d, reset_register=False)

        assert _key(a) in _stored_keys_at_run['e']

    def test_outputs_are_kept_when_a_task_may_yield_dynamic_dependencies(self) -> None:
        # If a were released after b finished, the yield of a would make the scheduler re-run the yielding task over and over
        # until a can be scheduled again.
        _yield_task_runs.clear()
        a = _InMemoryTask(name='a')
        task = _YieldTask(static=_InMemoryTask(name='b', upstream=[a]), dynamic=a)

        assert gokart.build(task, reset_register=False) == 'baa'
        assert len(_yield_task_runs) == 1

    @pytest.mark.parametrize('cache_task_completion', [False, True])
    def test_outputs_are_kept_when_a_decorated_run_may_yield_dynamic_dependencies(self, cache_task_completion: bool) -> None:
        _yield_task_runs.clear()
        a = _InMemoryTask(name='a')
        task = _DecoratedYieldTask(static=_InMemoryTask(name='b', upstream=[a]), dynamic=a)

        config = get_config()
        config.set('gokart_worker', 'cache_task_completion', str(cache_task_completion))
        try:
            assert gokart.build(task, reset_register=False) == 'baa'
        finally:
            config.remove_option('gokart_worker', 'cache_task_completion')
        assert len(_yield_task_runs) == 1

    def test_released_output_is_recomputed_when_required_by_an_undetected_yield(self) -> None:
        a = _InMemoryTask(name='a')
        task = _HelperYieldTask(static=_InMemoryTask(name='b', upstream=[a]), dynamic=a)

        config = get_config()
        config.set('gokart_worker', 'cache_task_completion', 'True')
        # WHY: the scheduler refuses to re-run a DONE task for 10 seconds by default.
        config.set('scheduler', 'stable_done_cooldown_secs', '0')
        try:
            assert gokart.build(task, reset_register=False) == 'baa'
        finally:
            config.remove_option('gokart_worker', 'cache_task_completion')
            config.remove_option('scheduler', 'stable_done_cooldown_secs')

    @pytest.mark.parametrize('check', ['strict_check', 'modification_time_check'])
    def test_outputs_are_kept_when_complete_checks_dependencies(self, check: str) -> None:
        # With these checks, complete() of c checks that the output of a (a dependency of b) still exists.
        check_param: dict[str, Any] = {check: True}
        a = _InMemoryTask(name='a', **check_param)
        b = _InMemoryTask(name='b', upstream=[a], **check_param)
        c = _InMemoryTask(name='c', upstream=[b], **check_param)

        config = get_config()
        config.set('gokart_worker', 'check_complete_on_run', 'True')
        try:
            assert gokart.build(c, reset_register=False) == 'cba'
        finally:
            config.remove_option('gokart_worker', 'check_complete_on_run')
