import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import luigi
import pytest
from luigi import scheduler
from luigi.configuration import get_config

import gokart
from gokart.build import GokartBuildError, WorkerSchedulerFactory
from gokart.in_memory import InMemoryTargetError, make_in_memory_target
from gokart.in_memory.repository import in_memory_scope
from gokart.worker import Worker, gokart_worker

_run_values: list[int] = []


class _FixedKeyTask(gokart.TaskOnKart[int]):
    task_namespace = __name__
    value: luigi.IntParameter = luigi.IntParameter()

    def output(self):
        return make_in_memory_target('fixed_key')

    def run(self):
        _run_values.append(self.value)
        self.dump(self.value)


_blocking_task_started = threading.Event()
_blocking_task_may_finish = threading.Event()


class _BlockingInMemoryTask(gokart.TaskOnKart[str]):
    """Keeps its gokart.build running until the test lets it finish."""

    task_namespace = __name__

    def output(self):
        return self.make_target('blocking.pkl', persist=False)

    def run(self):
        _blocking_task_started.set()
        assert _blocking_task_may_finish.wait(timeout=10)
        self.dump('blocking')


class _FileTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def run(self):
        self.dump('file')


class _TimeoutTask(_FixedKeyTask):
    task_namespace = __name__
    worker_timeout = 60


class _NoTimeoutTask(_FixedKeyTask):
    task_namespace = __name__
    worker_timeout = 0


class _BrokenOutputTask(gokart.TaskOnKart[int]):
    task_namespace = __name__

    def output(self):
        raise RuntimeError('broken output')


@pytest.fixture(autouse=True)
def _clear_run_values() -> None:
    _run_values.clear()


class TestBuildWithInMemoryTarget:
    def test_build_does_not_reuse_data_from_previous_build(self) -> None:
        assert gokart.build(_FixedKeyTask(value=1), reset_register=False) == 1
        assert gokart.build(_FixedKeyTask(value=2), reset_register=False) == 2

    def test_error_in_output_is_reported_as_build_error(self) -> None:
        with pytest.raises(GokartBuildError):
            gokart.build(_BrokenOutputTask(), reset_register=False)


class TestUnsupportedCombinations:
    def test_multiple_workers(self) -> None:
        with pytest.raises(InMemoryTargetError, match='worker'):
            gokart.build(_FixedKeyTask(value=1), reset_register=False, workers=2)
        assert _run_values == []

    def test_worker_timeout_of_task(self) -> None:
        with pytest.raises(InMemoryTargetError, match='worker_timeout'):
            gokart.build(_TimeoutTask(value=1), reset_register=False)
        assert _run_values == []

    def test_worker_timeout_in_config(self) -> None:
        config = get_config()
        config.set('gokart_worker', 'timeout', '60')
        try:
            with pytest.raises(InMemoryTargetError, match='worker_timeout'):
                gokart.build(_FixedKeyTask(value=1), reset_register=False)
        finally:
            config.remove_option('gokart_worker', 'timeout')
        assert _run_values == []

    def test_worker_timeout_of_task_overrides_config(self) -> None:
        config = get_config()
        config.set('gokart_worker', 'timeout', '60')
        try:
            assert gokart.build(_NoTimeoutTask(value=1), reset_register=False) == 1
        finally:
            config.remove_option('gokart_worker', 'timeout')

    def test_force_multiprocessing(self) -> None:
        with (
            in_memory_scope(),
            Worker(scheduler=scheduler.Scheduler(), config=gokart_worker(force_multiprocessing=True)) as worker,
            pytest.raises(InMemoryTargetError, match='force_multiprocessing'),
        ):
            worker.add(_FixedKeyTask(value=1))
        assert _run_values == []

    def test_should_lock_run_with_make_in_memory_target(self, lock_config: Callable[..., None]) -> None:
        lock_config(url='redis://localhost:6379')
        with pytest.raises(InMemoryTargetError, match='should_lock_run'):
            gokart.build(_FixedKeyTask(value=1, should_lock_run=True), reset_register=False)
        assert _run_values == []

    def test_outside_gokart_build(self) -> None:
        with pytest.raises(InMemoryTargetError, match=r'_FixedKeyTask.*gokart\.build'):
            luigi.build([_FixedKeyTask(value=1)], local_scheduler=True, worker_scheduler_factory=WorkerSchedulerFactory())
        assert _run_values == []


class TestConcurrentBuildsInThreads:
    @pytest.fixture
    def build_in_another_thread(self):
        _blocking_task_started.clear()
        _blocking_task_may_finish.clear()
        config = get_config()
        # WHY: the worker can install its signal handler only in the main thread.
        config.set('gokart_worker', 'no_install_shutdown_handler', 'True')
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(gokart.build, _BlockingInMemoryTask(), reset_register=False)
                assert _blocking_task_started.wait(timeout=10)
                try:
                    yield
                finally:
                    _blocking_task_may_finish.set()
                assert future.result() == 'blocking'
        finally:
            config.remove_option('gokart_worker', 'no_install_shutdown_handler')

    @pytest.mark.usefixtures('build_in_another_thread')
    def test_build_without_in_memory_target_is_not_affected(self, tmp_path: Path) -> None:
        assert gokart.build(_FileTask(workspace_directory=str(tmp_path)), reset_register=False) == 'file'

    @pytest.mark.usefixtures('build_in_another_thread')
    def test_build_with_in_memory_target_raises(self) -> None:
        with pytest.raises(InMemoryTargetError, match=r'_FixedKeyTask.*Another thread'):
            gokart.build(_FixedKeyTask(value=1), reset_register=False)
        assert _run_values == []


_inner_build_running = threading.Event()
_outer_thread_loaded = threading.Event()


class _InnerBuildTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def output(self):
        return self.make_target('inner.pkl', persist=False)

    def run(self):
        _inner_build_running.set()
        assert _outer_thread_loaded.wait(timeout=10)
        self.dump('inner')


class _OuterDependencyTask(gokart.TaskOnKart[str]):
    task_namespace = __name__

    def output(self):
        return self.make_target('outer_dependency.pkl', persist=False)

    def run(self):
        self.dump('outer dependency')


class _OuterTaskWithNestedBuild(gokart.TaskOnKart[str]):
    """Loads its input in a thread it started while it runs a nested gokart.build."""

    task_namespace = __name__

    def requires(self):
        return _OuterDependencyTask()

    def output(self):
        return self.make_target('outer.pkl', persist=False)

    def run(self):
        loaded: list[str] = []

        def load_in_thread() -> None:
            try:
                assert _inner_build_running.wait(timeout=10)
                loaded.append(self.load())
            finally:
                _outer_thread_loaded.set()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(load_in_thread)
            inner = gokart.build(_InnerBuildTask(), reset_register=False)
            future.result()
        self.dump(f'{loaded[0]} / {inner}')


class _OuterTaskDumpingInThread(_OuterTaskWithNestedBuild):
    """Dumps its output in a thread it started while it runs a nested gokart.build."""

    task_namespace = __name__

    def run(self):
        def dump_in_thread() -> None:
            try:
                assert _inner_build_running.wait(timeout=10)
                self.dump(f'dumped during nested build from {self.load()}')
            finally:
                _outer_thread_loaded.set()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(dump_in_thread)
            gokart.build(_InnerBuildTask(), reset_register=False)
            future.result()


class TestNestedBuild:
    @pytest.fixture(autouse=True)
    def _reset_events(self) -> None:
        _inner_build_running.clear()
        _outer_thread_loaded.clear()

    def test_thread_started_by_outer_task_sees_outer_store_during_nested_build(self) -> None:
        assert gokart.build(_OuterTaskWithNestedBuild(), reset_register=False) == 'outer dependency / inner'

    def test_thread_started_by_outer_task_dumps_to_outer_store_during_nested_build(self) -> None:
        assert gokart.build(_OuterTaskDumpingInThread(), reset_register=False) == 'dumped during nested build from outer dependency'
