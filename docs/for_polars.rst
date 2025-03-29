For Pandas
==========

Gokart also has features for Polars. It is enabled by installing extra using following command:

.. code:: sh

    pip install gokart[polars]


You need to set the environment variable ``GOKART_DATAFRAME_FRAMWORK_POLARS_ENABLED`` as ``true`` and you can use Polars for the most of the file format used in :func:`~gokart.task.TaskOnKart.load` and :func:`~gokart.task.TaskOnKart.dump` feature.
