For Pandas
==========

Gokart has several features for Pandas.


Pandas Type Config
------------------

Pandas has a feature that converts the type of column(s) automatically. This feature sometimes cause wrong result. To avoid unintentional type conversion of pandas, we can specify a column name to check the type of Task input and output in gokart.


.. code:: python

    from typing import Any, Dict
    import pandas as pd
    import gokart


    # Please define a class which inherits `gokart.PandasTypeConfig`.
    class SamplePandasTypeConfig(gokart.PandasTypeConfig):

        @classmethod
        def type_dict(cls) -> Dict[str, Any]:
            return {'int_column': int}


    class SampleTask(gokart.TaskOnKart):

        def run(self):
            # [PandasTypeError] because expected type is `int`, but `str` is passed.
            df = pd.DataFrame(dict(int_column=['a']))
            self.dump(df)

This is useful when dataframe has nullable columns because pandas auto-conversion often fails in such case.

Easy to Load DataFrame
----------------------

The :func:`~gokart.task.TaskOnKart.load_data_frame` method is used to load input ``pandas.DataFrame``.

.. code:: python

    def requires(self):
        return MakeDataFrameTask()

    def run(self):
        df = self.load_data_frame(required_columns={'colA', 'colB'}, drop_columns=True)

This allows us to omit ``reset_index`` and ``drop`` when loading. If there is a missing column in an example above, ``AssertionError`` will be raised. This feature is useful for pipelines based on pandas.

Please refer to :func:`~gokart.task.TaskOnKart.load_data_frame`.


Fail on empty DataFrame
-----------------------

The :func:`~gokart.task.TaskOnKart.fail_on_empty_dump` method is `AssertionError` on trying to dump empty ``pandas.DataFrame``.

.. code:: python

    def run(self):
        df = pd.DataFrame()
        self.fail_on_empty_dump(df)  # AssertionError

Empty caches sometimes hide bugs and let us spend much time debugging. This feature notifies us some bugs (including wrong datasources) in the early stage.

Please refer to :func:`~gokart.task.TaskOnKart.fail_on_empty_dump`.
