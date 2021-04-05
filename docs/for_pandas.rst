For Pandas
==========

Gokart has several features for Pandas.


Pandas Type Config
------------------

Pandas often estimates and changes the type automatically. In gokart, specify a column name to check the type of Task input and output.


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

This is useful when developing batches.


Easy to Load DataFrame
----------------------

The :func:`~gokart.task.TaskOnKart.load_data_frame` method is used to load input ``pandas.DataFrame``.

.. code:: python

    def requires(self):
        return MakeDataFrameTask()

    def run(self):
        df = self.load_data_frame(required_columns={'colA', 'colB'}, drop_columns=True)

This allows us to omit ``reset_index`` and ``drop`` when loading. And if there is a missing column, ``AssertionError`` will be raised. Useful for pipelines based on pandas.

Please refer to :func:`~gokart.task.TaskOnKart.load_data_frame`.


Fail on empty DataFrame
-----------------------

The :func:`~gokart.task.TaskOnKart.fail_on_empty_dump` method is `AssertionError` on trying to dump empty ``pandas.DataFrame``.

.. code:: python

    def run(self):
        df = pd.DataFrame()
        self.fail_on_empty_dump(df)  # AssertionError

Empty caches sometimes hide bugs and let us spend much time debugging. This feature notice us some bugs (including wrong datasources) in the early stage.

Please refer to :func:`~gokart.task.TaskOnKart.fail_on_empty_dump`.
