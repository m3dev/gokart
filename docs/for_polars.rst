For Pandas
==========

Gokart also has features for Polars. It is enabled by installing extra using following command:

.. code:: sh

    pip install gokart[polars]


You need to set the environment variable ``GOKART_DATAFRAME_FRAMEWORK`` as ``polars`` and you can use Polars for the most of the file format used in :func:`~gokart.task.TaskOnKart.load` and :func:`~gokart.task.TaskOnKart.dump` feature.
If you don't set ``GOKART_DATAFRAME_FRAMEWORK`` or set it as ``pandas``, you can use pandas for it.
