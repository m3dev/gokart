Polars Support
==============

Gokart supports Polars DataFrames alongside pandas DataFrames for DataFrame-based file processors. This allows gradual migration from pandas to Polars or using both libraries simultaneously in your data pipelines.


Installation
------------

Polars support is optional. Install it with:

.. code:: bash

    pip install gokart[polars]

Or install Polars separately:

.. code:: bash

    pip install polars


Basic Usage
-----------

To use Polars DataFrames with gokart, specify ``dataframe_type='polars'`` when creating file processors:

.. code:: python

    import polars as pl
    from gokart import TaskOnKart
    from gokart.file_processor import FeatherFileProcessor

    class MyPolarsTask(TaskOnKart[pl.DataFrame]):
        def output(self):
            return self.make_target(
                'path/to/target.feather',
                processor=FeatherFileProcessor(
                    store_index_in_feather=False,
                    dataframe_type='polars'
                )
            )

        def run(self):
            df = pl.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
            self.dump(df)


Supported File Processors
--------------------------

The following file processors support the ``dataframe_type`` parameter:

CsvFileProcessor
^^^^^^^^^^^^^^^^

.. code:: python

    from gokart.file_processor import CsvFileProcessor

    # For Polars
    processor = CsvFileProcessor(sep=',', encoding='utf-8', dataframe_type='polars')

    # For pandas (default)
    processor = CsvFileProcessor(sep=',', encoding='utf-8', dataframe_type='pandas')
    # or simply
    processor = CsvFileProcessor(sep=',', encoding='utf-8')


JsonFileProcessor
^^^^^^^^^^^^^^^^^

.. code:: python

    from gokart.file_processor import JsonFileProcessor

    # For Polars
    processor = JsonFileProcessor(orient='records', dataframe_type='polars')

    # For pandas (default)
    processor = JsonFileProcessor(orient='records', dataframe_type='pandas')


ParquetFileProcessor
^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from gokart.file_processor import ParquetFileProcessor

    # For Polars
    processor = ParquetFileProcessor(
        compression='gzip',
        dataframe_type='polars'
    )

    # For pandas (default)
    processor = ParquetFileProcessor(
        compression='gzip',
        dataframe_type='pandas'
    )


FeatherFileProcessor
^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from gokart.file_processor import FeatherFileProcessor

    # For Polars
    processor = FeatherFileProcessor(
        store_index_in_feather=False,
        dataframe_type='polars'
    )

    # For pandas (default)
    processor = FeatherFileProcessor(
        store_index_in_feather=True,
        dataframe_type='pandas'
    )

.. note::
    The ``store_index_in_feather`` parameter is pandas-specific and is ignored when using Polars.


Using Pandas and Polars Together
---------------------------------

Since projects often migrate from pandas gradually, gokart allows you to use both pandas and Polars simultaneously:

.. code:: python

    import pandas as pd
    import polars as pl
    from gokart import TaskOnKart
    from gokart.file_processor import FeatherFileProcessor

    class PandasTask(TaskOnKart[pd.DataFrame]):
        """Task that outputs pandas DataFrame"""
        def output(self):
            return self.make_target(
                'path/to/pandas_output.feather',
                processor=FeatherFileProcessor(
                    store_index_in_feather=False,
                    dataframe_type='pandas'
                )
            )

        def run(self):
            df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
            self.dump(df)

    class PolarsTask(TaskOnKart[pl.DataFrame]):
        """Task that outputs Polars DataFrame"""
        def requires(self):
            return PandasTask()

        def output(self):
            return self.make_target(
                'path/to/polars_output.feather',
                processor=FeatherFileProcessor(
                    store_index_in_feather=False,
                    dataframe_type='polars'
                )
            )

        def run(self):
            # Load pandas DataFrame and convert to Polars
            pandas_df = self.load()  # Returns pandas DataFrame
            polars_df = pl.from_pandas(pandas_df)

            # Process with Polars
            result = polars_df.with_columns(
                (pl.col('a') * 2).alias('a_doubled')
            )

            self.dump(result)


Default Behavior
----------------

When ``dataframe_type`` is not specified, file processors default to ``'pandas'`` for backward compatibility:

.. code:: python

    # These are equivalent
    processor = CsvFileProcessor(sep=',')
    processor = CsvFileProcessor(sep=',', dataframe_type='pandas')


Important Notes
---------------

**File Format Compatibility**

Files created with Polars processors can be read by pandas processors and vice versa. The underlying file formats (CSV, JSON, Parquet, Feather) are library-agnostic.

**Pandas-specific Features**

Some pandas-specific features are not available with Polars:

- ``store_index_in_feather`` parameter in ``FeatherFileProcessor`` is ignored for Polars
- ``engine`` parameter in ``ParquetFileProcessor`` is ignored for Polars (uses Polars' default)

**Error Handling**

If you specify ``dataframe_type='polars'`` but Polars is not installed, you'll get an ``ImportError`` with installation instructions:

.. code:: text

    ImportError: polars is required for dataframe_type='polars'. Install with: pip install polars


Migration Strategy
------------------

Recommended approach for migrating from pandas to Polars:

1. Install Polars: ``pip install gokart[polars]``
2. Create new tasks using ``dataframe_type='polars'``
3. Keep existing tasks with ``dataframe_type='pandas'`` or default behavior
4. Gradually migrate tasks as needed
5. Convert DataFrames between libraries using ``pl.from_pandas()`` and ``df.to_pandas()`` when necessary
