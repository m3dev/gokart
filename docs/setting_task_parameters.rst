Setting Task Parameters
======================================================

There are several ways to set task parameters.

1. Set parameter from command line
--------------------------------------
.. code:: sh

    python main.py sample.SomeTask --SomeTask-param=Hello

Parameter of each task can be set as a command line parameter in ``--[task name]-[parameter name]=[value]`` format.

2. Set parameter at config file
--------------------------------------
::

    [sample.SomeTask]
    param = Hello

Above config file (``config.conf``) must be read before ``gokart.run()`` as in the following example. 

.. code:: python

    if __name__ == '__main__':
        luigi.configuration.core.PARSER = 'ini'
        assert luigi.configuration.add_config_path('./conf/config.conf')
        gokart.run()

3. Set parameter at upstream task
--------------------------------------
.. code:: python

    class UpstreamTask(gokart.TaskOnKart):
        def requires(self):
            return dict(sometask=SomeTask(param='Hello'))


4. Inherit parameter from other task
--------------------------------------
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
