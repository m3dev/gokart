Setting Task Parameters
======================================================

There are several ways to set task parameters. 
Please refer to `luigi document <https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html>`_ for a list of parameter types.

There are several ways to do this.

- command line
- config file & enviroment variables
- upstream task
- inherits_config_params

There is also a parameter provided by gokart, which is described in Advanced Features section.


Set parameter from command line
--------------------------------------
.. code:: sh

    python main.py sample.SomeTask --SomeTask-param=Hello

Parameter of each task can be set as a command line parameter in ``--[task name]-[parameter name]=[value]`` format.


Set parameter at config file
--------------------------------------
::

    [sample.SomeTask]
    param = Hello

Above config file (``config.ini``) must be read before ``gokart.run()`` as in the following: 

.. code:: python

    if __name__ == '__main__':
        gokart.add_config('./conf/config.ini')
        gokart.run()


It can also be loaded from environment variable in the following:

::

    [sample.SomeTask]
    param=%(PARAMS)s

    [TaskOnKart]
    workspace_directory=%(WORKSPACE_DIRECTORY)s

The advantage of using environment variables is that important information is not logged and common settings can be used.


Set parameter at upstream task
--------------------------------------

Parameters can be set from upstream, as in a typical pipeline.

.. code:: python

    class UpstreamTask(gokart.TaskOnKart):
        def requires(self):
            return dict(sometask=SomeTask(param='Hello'))


Inherit parameter from other task
--------------------------------------

Parameters can be set ``@inherits_config_params`` decorator.

.. code:: python

    class MasterConfig(luigi.Config):
        param: str = luigi.Parameter()
        param2: str = luigi.Parameter()

    @inherits_config_params(MasterConfig)
    class SomeTask(gokart.TaskOnKart):
        param: str = luigi.Parameter()


This is useful when multiple tasks has the same parameter, since parameter settings of ``MasterConfig`` will be inherited to all tasks decorated with ``@inherits_config_params(MasterConfig)``.

Note that parameters which exist in both ``MasterConfig`` and ``SomeTask`` will be inherited.
In the above example, ``param2`` will not be available in ``SomeTask``, since ``SomeTask`` does not have ``param2`` parameter.


Advanced Features
---------------------

gokart.TaskInstanceParameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


gokart.ListTaskInstanceParameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


gokart.ExplicitBoolParameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

