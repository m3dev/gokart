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


    class SampleTask(gokart.TaskOnKart[pd.DataFrame]):

        def run(self):
            # [PandasTypeError] because expected type is `int`, but `str` is passed.
            df = pd.DataFrame(dict(int_column=['a']))
            self.dump(df)

This is useful when dataframe has nullable columns because pandas auto-conversion often fails in such case.

Easy to Load DataFrame
----------------------

The :func:`~gokart.task.TaskOnKart.load` method is used to load input ``pandas.DataFrame``.

.. code:: python

    def requires(self):
        return MakeDataFrameTask()

    def run(self):
        df = self.load()

Please refer to :func:`~gokart.task.TaskOnKart.load`.


Fail on empty DataFrame
-----------------------

When the :attr:`~gokart.task.TaskOnKart.fail_on_empty_dump` parameter is true, the :func:`~gokart.task.TaskOnKart.dump()` method raises :class:`~gokart.task.EmptyDumpError` on trying to dump empty ``pandas.DataFrame``.


.. code:: python

    import gokart


    class EmptyTask(gokart.TaskOnKart):
        def run(self):
            df = pd.DataFrame()
            self.dump(df)


::

    $ python main.py EmptyTask --fail-on-empty-dump true
    # EmptyDumpError
    $ python main.py EmptyTask
    # Task will be ran and outputs an empty dataframe


Empty caches sometimes hide bugs and let us spend much time debugging. This feature notifies us some bugs (including wrong datasources) in the early stage.

Please refer to :attr:`~gokart.task.TaskOnKart.fail_on_empty_dump`.
