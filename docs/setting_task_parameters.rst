============================
Setting Task Parameters
============================

There are several ways to set task parameters. 

- Set parameter from command line
- Set parameter at config file
- Set parameter at upstream task
- Inherit parameter from other task


Set parameter from command line
==================================
.. code:: sh

    python main.py sample.SomeTask --SomeTask-param=Hello

Parameter of each task can be set as a command line parameter in ``--[task name]-[parameter name]=[value]`` format.


Set parameter at config file
==================================
::

    [sample.SomeTask]
    param = Hello

Above config file (``config.ini``) must be read before ``gokart.run()`` as the following code: 

.. code:: python

    if __name__ == '__main__':
        gokart.add_config('./conf/config.ini')
        gokart.run()


It can also be loaded from environment variable as the following code:

::

    [sample.SomeTask]
    param=${PARAMS}

    [TaskOnKart]
    workspace_directory=${WORKSPACE_DIRECTORY}

The advantages of using environment variables are 1) important information will not be logged 2) common settings can be used.


Set parameter at upstream task
==================================

Parameters can be set at the upstream task, as in a typical pipeline.

.. code:: python

    class UpstreamTask(gokart.TaskOnKart):
        def requires(self):
            return dict(sometask=SomeTask(param='Hello'))


Inherit parameter from other task
==================================

Parameter values can be inherited from other task using ``@inherits_config_params`` decorator.

.. code:: python

    class MasterConfig(luigi.Config):
        param: str = luigi.Parameter()
        param2: str = luigi.Parameter()

    @inherits_config_params(MasterConfig)
    class SomeTask(gokart.TaskOnKart):
        param: str = luigi.Parameter()


This is useful when multiple tasks has the same parameter. In the above example, parameter settings of ``MasterConfig`` will be inherited to all tasks decorated with ``@inherits_config_params(MasterConfig)`` as ``SomeTask``.

Note that only parameters which exist in both ``MasterConfig`` and ``SomeTask`` will be inherited.
In the above example, ``param2`` will not be available in ``SomeTask``, since ``SomeTask`` does not have ``param2`` parameter.

.. code:: python

    class MasterConfig(luigi.Config):
        param: str = luigi.Parameter()
        param2: str = luigi.Parameter()

    @inherits_config_params(MasterConfig, parameter_alias={'param2': 'param3'})
    class SomeTask(gokart.TaskOnKart):
        param3: str = luigi.Parameter()


You may also set a parameter name alias by setting ``parameter_alias``.
``parameter_alias`` must be a dictionary of key: inheriting task's parameter name, value: decorating task's parameter name.

In the above example, ``SomeTask.param3`` will be set to same value as ``MasterConfig.param2``.
