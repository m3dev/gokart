"""gokart without pandas: pandas is imported only by the features that need it (#674)."""

from __future__ import annotations

import importlib.metadata
import io
import subprocess
import sys
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from packaging.requirements import Requirement

import gokart
from gokart.file_processor import (
    CsvFileProcessor,
    FeatherFileProcessor,
    JsonFileProcessor,
    NdjsonFileProcessor,
    ParquetFileProcessor,
    make_file_processor,
)
from gokart.testing import assert_frame_contents_equal
from gokart.tree.task_info import make_task_info_as_table
from gokart.utils import load_dill_with_pandas_backward_compatibility

_INSTALL_HINT = r'pip install gokart\[pandas\]'


def _run_python(code: str, cwd: Path | None = None) -> None:
    subprocess.run([sys.executable, '-c', textwrap.dedent(code)], check=True, cwd=cwd)


def test_pandas_and_pyarrow_are_installed_only_with_the_pandas_extra() -> None:
    requirements = [Requirement(r) for r in importlib.metadata.requires('gokart') or []]
    # evaluate the markers instead of skipping them, so that e.g. `pandas; python_version < "3.12"` would still count as required
    required = {r.name for r in requirements if r.marker is None or r.marker.evaluate({'extra': ''})}
    from_pandas_extra = {r.name for r in requirements if r.marker is not None and r.marker.evaluate({'extra': 'pandas'})}

    assert {'pandas', 'pyarrow'}.isdisjoint(required)
    assert {'pandas', 'pyarrow'} <= from_pandas_extra
    assert 'numpy' in required


def test_import_gokart_does_not_import_pandas() -> None:
    _run_python(
        """
        import sys
        import gokart
        import gokart.errors
        import gokart.file_processor
        import gokart.testing
        import gokart.tree.task_info
        assert 'pandas' not in sys.modules, 'import gokart imported pandas'
        """
    )


def test_build_without_pandas(tmp_path: Path) -> None:
    _run_python(
        """
        import sys
        sys.modules['pandas'] = None

        import polars as pl
        import polars.testing

        import gokart


        class _Numbers(gokart.TaskOnKart[dict]):
            def output(self):
                return self.make_target('numbers.pkl')

            def run(self):
                self.dump({'x': 1})


        class _Frame(gokart.TaskOnKart[pl.DataFrame]):
            def output(self):
                return self.make_target('frame.parquet')

            def run(self):
                self.dump(pl.DataFrame({'a': [1, 2]}))


        class _Join(gokart.TaskOnKart[pl.DataFrame]):
            numbers = gokart.TaskInstanceParameter()
            frame = gokart.TaskInstanceParameter()

            def output(self):
                return self.make_target('join.csv')

            def run(self):
                self.dump(self.load('frame').with_columns(x=pl.lit(self.load('numbers')['x'])))


        result = gokart.build(_Join(numbers=_Numbers(), frame=_Frame(), workspace_directory='./resources'), reset_register=False)
        polars.testing.assert_frame_equal(result, pl.DataFrame({'a': [1, 2], 'x': [1, 1]}), check_dtypes=False)
        """,
        cwd=tmp_path,
    )


@pytest.fixture
def without_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, 'pandas', None)


class _AnyTask(gokart.TaskOnKart[Any]):
    task_namespace = __name__


_PANDAS_FEATURES: list[Any] = [
    pytest.param(lambda: CsvFileProcessor(), id='csv'),
    pytest.param(lambda: JsonFileProcessor(), id='json'),
    pytest.param(lambda: NdjsonFileProcessor(), id='ndjson'),
    pytest.param(lambda: ParquetFileProcessor(), id='parquet'),
    pytest.param(lambda: FeatherFileProcessor(store_index_in_feather=True), id='feather'),
    pytest.param(lambda: make_file_processor('data.tsv'), id='make_file_processor'),
    pytest.param(lambda: _AnyTask().make_target('data.csv'), id='make_target'),
    pytest.param(
        lambda: _AnyTask().make_large_data_frame_target('data.zip'),
        id='make_large_data_frame_target',
        marks=pytest.mark.filterwarnings('ignore:make_large_data_frame_target is deprecated:FutureWarning'),
    ),
    pytest.param(lambda: make_task_info_as_table(_AnyTask()), id='make_task_info_as_table'),
    pytest.param(lambda: assert_frame_contents_equal(object(), object()), id='assert_frame_contents_equal'),  # pyright: ignore[reportArgumentType]
]


@pytest.mark.usefixtures('without_pandas')
@pytest.mark.parametrize('use_pandas_feature', _PANDAS_FEATURES)
def test_pandas_feature_raises_install_hint(use_pandas_feature: Callable[[], object]) -> None:
    # The hint is raised when the target or the processor is created, not when a task dumps its output after run().
    with pytest.raises(ImportError, match=_INSTALL_HINT):
        use_pandas_feature()


_NON_PANDAS_EXTENSIONS = ['.txt', '.ini', '.pkl', '.gz', '.xml', '.npz', '.png', '.jpg']


@pytest.mark.usefixtures('without_pandas')
@pytest.mark.parametrize('extension', _NON_PANDAS_EXTENSIONS)
def test_make_file_processor_does_not_need_pandas(extension: str) -> None:
    make_file_processor(f'data{extension}')


@pytest.mark.usefixtures('without_pandas')
@pytest.mark.parametrize('dataframe_type', ['polars', 'polars-lazy'])
def test_polars_processors_do_not_need_pandas(dataframe_type: Any) -> None:
    for extension in ['.csv', '.tsv', '.json', '.ndjson', '.parquet', '.feather']:
        make_file_processor(f'data{extension}', dataframe_type=dataframe_type)


@pytest.mark.usefixtures('without_pandas')
def test_dump_checks_do_not_need_pandas(tmp_path: Path) -> None:
    # PandasTypeConfigMap.check, fail_on_empty_dump and the in-memory copy only look for a DataFrame, which cannot exist without pandas.
    task = _AnyTask(fail_on_empty_dump=True, workspace_directory=str(tmp_path))
    target = task.make_target('empty.pkl')
    task.dump([], target)
    assert target.load() == []


@pytest.mark.usefixtures('without_pandas')
def test_load_pickle_keeps_dill_error_without_pandas() -> None:
    # pd.read_pickle is only a fallback for pickles of older pandas versions; without pandas, the error of dill is the real cause.
    with pytest.raises(Exception) as exc_info:
        load_dill_with_pandas_backward_compatibility(io.BytesIO(b'not a pickle'))
    assert not isinstance(exc_info.value, ImportError)
