from typing import Dict, Any

import gokart
import pandas as pd


# Please define a class which inherits `gokart.PandasTypeConfig`.
# **In practice, please import `SamplePandasTypeConfig` in `__init__`.**
class SamplePandasTypeConfig(gokart.PandasTypeConfig):
    task_namespace = 'sample_pandas_type_check'

    @classmethod
    def type_dict(cls) -> Dict[str, Any]:
        return {'int_column': int}


class SampleTask(gokart.TaskOnKart):
    # Please set the same `task_namespace` as `SamplePandasTypeConfig`.
    task_namespace = 'sample_pandas_type_check'

    def run(self):
        df = pd.DataFrame(dict(int_column=['a']))
        self.dump(df)  # This line causes PandasTypeError, because expected type is `int`, but `str` is passed.


if __name__ == '__main__':
    gokart.run(['sample_pandas_type_check.SampleTask', '--local-scheduler', '--rerun'])
