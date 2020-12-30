from datetime import datetime, date
from typing import Dict, Any
from unittest import TestCase

import numpy as np
import pandas as pd

from gokart import PandasTypeConfig
from gokart.pandas_type_config import PandasTypeError


class _DummyPandasTypeConfig(PandasTypeConfig):
    @classmethod
    def type_dict(cls) -> Dict[str, Any]:
        return {'int_column': int, 'datetime_column': datetime, 'array_column': np.ndarray}


class TestPandasTypeConfig(TestCase):
    def test_int_fail(self):
        df = pd.DataFrame(dict(int_column=['1']))
        with self.assertRaises(PandasTypeError):
            _DummyPandasTypeConfig().check(df)

    def test_int_success(self):
        df = pd.DataFrame(dict(int_column=[1]))
        _DummyPandasTypeConfig().check(df)

    def test_datetime_fail(self):
        df = pd.DataFrame(dict(datetime_column=[date(2019, 1, 12)]))
        with self.assertRaises(PandasTypeError):
            _DummyPandasTypeConfig().check(df)

    def test_datetime_success(self):
        df = pd.DataFrame(dict(datetime_column=[datetime(2019, 1, 12, 0, 0, 0)]))
        _DummyPandasTypeConfig().check(df)

    def test_array_fail(self):
        df = pd.DataFrame(dict(array_column=[[1, 2]]))
        with self.assertRaises(PandasTypeError):
            _DummyPandasTypeConfig().check(df)

    def test_array_success(self):
        df = pd.DataFrame(dict(array_column=[np.array([1, 2])]))
        _DummyPandasTypeConfig().check(df)
