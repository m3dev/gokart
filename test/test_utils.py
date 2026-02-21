import unittest
from typing import TYPE_CHECKING

import pandas as pd
import pytest

from gokart.task import TaskOnKart
from gokart.utils import flatten, get_dataframe_type_from_task, map_flattenable_items

if TYPE_CHECKING:
    import polars as pl

try:
    import polars as pl

    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


class TestFlatten(unittest.TestCase):
    def test_flatten_dict(self):
        self.assertEqual(flatten({'a': 'foo', 'b': 'bar'}), ['foo', 'bar'])

    def test_flatten_list(self):
        self.assertEqual(flatten(['foo', ['bar', 'troll']]), ['foo', 'bar', 'troll'])

    def test_flatten_str(self):
        self.assertEqual(flatten('foo'), ['foo'])

    def test_flatten_int(self):
        self.assertEqual(flatten(42), [42])

    def test_flatten_none(self):
        self.assertEqual(flatten(None), [])


class TestMapFlatten(unittest.TestCase):
    def test_map_flattenable_items(self):
        self.assertEqual(map_flattenable_items(lambda x: str(x), {'a': 1, 'b': 2}), {'a': '1', 'b': '2'})
        self.assertEqual(
            map_flattenable_items(lambda x: str(x), (1, 2, 3, (4, 5, (6, 7, {'a': (8, 9, 0)})))),
            ('1', '2', '3', ('4', '5', ('6', '7', {'a': ('8', '9', '0')}))),
        )
        self.assertEqual(
            map_flattenable_items(
                lambda x: str(x),
                {'a': [1, 2, 3, '4'], 'b': {'c': True, 'd': {'e': 5}}},
            ),
            {'a': ['1', '2', '3', '4'], 'b': {'c': 'True', 'd': {'e': '5'}}},
        )


class TestGetDataFrameTypeFromTask(unittest.TestCase):
    """Tests for get_dataframe_type_from_task function."""

    def test_pandas_dataframe_from_instance(self):
        """Test detecting pandas DataFrame from task instance."""

        class _PandasTaskInstance(TaskOnKart[pd.DataFrame]):
            pass

        task = _PandasTaskInstance()
        self.assertEqual(get_dataframe_type_from_task(task), 'pandas')

    def test_pandas_dataframe_from_class(self):
        """Test detecting pandas DataFrame from task class."""

        class _PandasTaskClass(TaskOnKart[pd.DataFrame]):
            pass

        self.assertEqual(get_dataframe_type_from_task(_PandasTaskClass), 'pandas')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_polars_dataframe_from_instance(self):
        """Test detecting polars DataFrame from task instance."""

        class _PolarsTaskInstance(TaskOnKart[pl.DataFrame]):
            pass

        task = _PolarsTaskInstance()
        self.assertEqual(get_dataframe_type_from_task(task), 'polars')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_polars_dataframe_from_class(self):
        """Test detecting polars DataFrame from task class."""

        class _PolarsTaskClass(TaskOnKart[pl.DataFrame]):
            pass

        self.assertEqual(get_dataframe_type_from_task(_PolarsTaskClass), 'polars')

    def test_no_type_parameter_defaults_to_pandas(self):
        """Test that tasks without type parameter default to pandas."""

        # Create a class without __orig_bases__ by not using type parameters
        class PlainTask:
            pass

        task = PlainTask()
        self.assertEqual(get_dataframe_type_from_task(task), 'pandas')

    def test_non_taskonkart_class_defaults_to_pandas(self):
        """Test that non-TaskOnKart classes default to pandas."""

        class RegularClass:
            pass

        task = RegularClass()
        self.assertEqual(get_dataframe_type_from_task(task), 'pandas')

    def test_taskonkart_with_non_dataframe_type(self):
        """Test TaskOnKart with non-DataFrame type parameter defaults to pandas."""

        class _StringTask(TaskOnKart[str]):
            pass

        task = _StringTask()
        # Should default to pandas since str module is not 'pandas' or 'polars'
        self.assertEqual(get_dataframe_type_from_task(task), 'pandas')

    def test_nested_inheritance_pandas(self):
        """Test that nested inheritance without direct type parameter defaults to pandas."""

        class _BasePandasTask(TaskOnKart[pd.DataFrame]):
            pass

        class _DerivedPandasTask(_BasePandasTask):
            pass

        task = _DerivedPandasTask()
        # _DerivedPandasTask doesn't have its own __orig_bases__ with type parameter,
        # so it defaults to 'pandas'
        self.assertEqual(get_dataframe_type_from_task(task), 'pandas')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_nested_inheritance_polars(self):
        """Test detecting polars DataFrame type through nested inheritance."""

        class _BasePolarsTask(TaskOnKart[pl.DataFrame]):
            pass

        class _DerivedPolarsTask(_BasePolarsTask):
            pass

        task = _DerivedPolarsTask()
        # Function should detect 'polars' through the inheritance chain
        self.assertEqual(get_dataframe_type_from_task(task), 'polars')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_polars_lazyframe_from_instance(self):
        class _LazyTaskInstance(TaskOnKart[pl.LazyFrame]):
            pass

        task = _LazyTaskInstance()
        self.assertEqual(get_dataframe_type_from_task(task), 'polars-lazy')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_polars_lazyframe_from_class(self):
        class _LazyTaskClass(TaskOnKart[pl.LazyFrame]):
            pass

        self.assertEqual(get_dataframe_type_from_task(_LazyTaskClass), 'polars-lazy')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_nested_inheritance_polars_lazyframe(self):
        class _BaseLazyTask(TaskOnKart[pl.LazyFrame]):
            pass

        class _DerivedLazyTask(_BaseLazyTask):
            pass

        task = _DerivedLazyTask()
        self.assertEqual(get_dataframe_type_from_task(task), 'polars-lazy')

    @pytest.mark.skipif(not HAS_POLARS, reason='polars not installed')
    def test_nested_inheritance_polars_with_mixin(self):
        """Derived class with multiple bases should still detect polars through MRO."""

        class _Mixin:
            pass

        class _BasePolarsTaskWithMixin(TaskOnKart[pl.DataFrame]):
            pass

        # Multiple inheritance gives _DerivedTask its own __orig_bases__,
        # which shadows the parent's and doesn't contain TaskOnKart[...].
        class _DerivedTaskWithMixin(_BasePolarsTaskWithMixin, _Mixin):
            pass

        task = _DerivedTaskWithMixin()
        self.assertEqual(get_dataframe_type_from_task(task), 'polars')
