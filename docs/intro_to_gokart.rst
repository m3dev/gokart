Intro To Gokart
===============


Installation
------------

Within the activated Python environment, use the following command to install gokart.

.. code:: sh

    pip install gokart



Quickstart
----------

A minimal gokart tasks looks something like this:


.. code:: python

    import gokart

    class Example(gokart.TaskOnKart[str]):
        def run(self):
            self.dump('Hello, world!')

    task = Example()
    output = gokart.build(task)
    print(output)


``gokart.build`` return the result of dump by ``gokart.TaskOnKart``. The example will output the following.


.. code:: sh

    Hello, world!


``gokart`` records all the information needed for Machine Learning. By default, ``resources`` will be generated in the same directory as the script.

.. code:: sh

    $ tree resources/
    resources/
    ├── __main__
    │   └── Example_8441c59b5ce0113396d53509f19371fb.pkl
    └── log
        ├── module_versions
        │   └── Example_8441c59b5ce0113396d53509f19371fb.txt
        ├── processing_time
        │   └── Example_8441c59b5ce0113396d53509f19371fb.pkl
        ├── random_seed
        │   └── Example_8441c59b5ce0113396d53509f19371fb.pkl
        ├── task_log
        │   └── Example_8441c59b5ce0113396d53509f19371fb.pkl
        └── task_params
            └── Example_8441c59b5ce0113396d53509f19371fb.pkl


The result of dumping the task will be saved in the ``__name__`` directory.


.. code:: python

    import pickle

    with open('resources/__main__/Example_8441c59b5ce0113396d53509f19371fb.pkl', 'rb') as f:
        print(pickle.load(f))  # Hello, world!


That will be given hash value depending on the parameter of the task. This means that if you change the parameter of the task, the hash value will change, and change output file. This is very useful when changing parameters and experimenting. Please refer to :doc:`task_parameters` section for task parameters. Also see :doc:`task_on_kart` section for information on how to return this output destination.


In addition, the following files are automatically saved as ``log``.

- ``module_versions``: The versions of all modules that were imported when the script was executed. For reproducibility.
- ``processing_time``: The execution time of the task.
- ``random_seed``: This is random seed of python and numpy. For reproducibility in Machine Learning. Please refer to :doc:`task_settings` section.
- ``task_log``: This is the output of the task logger.
- ``task_params``: This is task's parameters. Please refer to :doc:`task_parameters` section.


How to running task
-------------------

Gokart has ``run`` and ``build`` methods for running task. Each has a different purpose.

- ``gokart.run``: uses arguments on the shell. return retcode.
- ``gokart.build``: uses inline code on jupyter notebook, IPython, and more. return task output.


.. note::

    It is not recommended to use ``gokart.run`` and ``gokart.build`` together in the same script. Because ``gokart.build`` will clear the contents of ``luigi.register``. It's the only way to handle duplicate tasks.


gokart.run
~~~~~~~~~~

The :func:`~gokart.run` is running on shell.

.. code:: python

    import gokart
    import luigi

    class SampleTask(gokart.TaskOnKart[str]):
        param = luigi.Parameter()

        def run(self):
            self.dump(self.param)

    gokart.run()


.. code:: sh

    python sample.py SampleTask --local-scheduler --param=hello


If you were to write it in Python, it would be the same as the following behavior.


.. code:: python

    gokart.run(['SampleTask', '--local-scheduler', '--param=hello'])


gokart.build
~~~~~~~~~~~~

The :func:`~gokart.build` is inline code.

.. code:: python

    import gokart
    import luigi

    class SampleTask(gokart.TaskOnKart[str]):
        param = luigi.Parameter()

        def run(self):
            self.dump(self.param)

    gokart.build(SampleTask(param='hello'), return_value=False)


To output logs of each tasks, you can pass `~log_level` parameter to `~gokart.build` as follows:

.. code:: python

    gokart.build(SampleTask(param='hello'), return_value=False, log_level=logging.DEBUG)


This feature is very useful for running `~gokart` on jupyter notebook.
When some tasks are failed, gokart.build raises GokartBuildError. If you have to get tracebacks, you should set `log_level` as `logging.DEBUG`.
