from abc import abstractmethod
from logging import getLogger
from typing import Any, Dict

import luigi
import numpy as np
import pandas as pd
from luigi.task_register import Register

logger = getLogger(__name__)


class PandasTypeError(Exception):
    pass


class PandasTypeConfig(luigi.Config):

    @classmethod
    @abstractmethod
    def type_dict(cls) -> Dict[str, Any]:
        pass

    @classmethod
    def check(cls, df: pd.DataFrame):
        for column_name, column_type in cls.type_dict().items():
            cls._check_column(df, column_name, column_type)

    @classmethod
    def _check_column(cls, df, column_name, column_type):
        if column_name not in df.columns:
            return

        if not np.all(list(map(lambda x: isinstance(x, column_type), df[column_name]))):
            not_match = next(filter(lambda x: not isinstance(x, column_type), df[column_name]))
            raise PandasTypeError(f'expected type is "{column_type}", but "{type(not_match)}" is passed in column "{column_name}".')


class PandasTypeConfigMap(luigi.Config):
    """To initialize this class only once, this inherits luigi.Config."""

    def __init__(self, *args, **kwargs) -> None:
        super(PandasTypeConfigMap, self).__init__(*args, **kwargs)
        task_names = Register.task_names()
        task_classes = [Register.get_task_cls(task_name) for task_name in task_names]
        self._map = {
            task_class.task_namespace: task_class
            for task_class in task_classes if issubclass(task_class, PandasTypeConfig) and task_class != PandasTypeConfig
        }

    def check(self, obj, task_namespace: str):
        if type(obj) == pd.DataFrame and task_namespace in self._map:
            self._map[task_namespace].check(obj)
