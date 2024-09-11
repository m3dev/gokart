Task Settings
=============

Task settings. Also please refer to :doc:`task_parameters` section.


Directory to Save Outputs
-------------------------

We can use both a local directory and the S3 to save outputs.
If you would like to use local directory, please set a local directory path to :attr:`~gokart.task.TaskOnKart.workspace_directory`. Please refer to :doc:`task_parameters` for how to set it up.

It is recommended to use the config file since it does not change much.

::

    # base.ini
    [TaskOnKart]
    workspace_directory=${TASK_WORKSPACE_DIRECTORY}

.. code:: python

    # main.py
    import gokart
    gokart.add_config('base.ini')


To use the S3 or GCS repository, please set the bucket path as ``s3://{YOUR_REPOSITORY_NAME}`` or ``gs://{YOUR_REPOSITORY_NAME}`` respectively.

If use S3 or GCS, please set credential information to Environment Variables.

.. code:: sh

    # S3
    export AWS_ACCESS_KEY_ID='~~~'  # AWS access key
    export AWS_SECRET_ACCESS_KEY='~~~'  # AWS secret access key

    # GCS
    export GCS_CREDENTIAL='~~~'  # GCS credential
    export DISCOVER_CACHE_LOCAL_PATH='~~~'  # The local file path of discover api cache.


Rerun task
----------

There are times when we want to rerun a task, such as when change script or on batch. Please use the ``rerun`` parameter or add an arbitrary parameter.


When set rerun as follows:

.. code:: python

    # rerun TaskA
    gokart.build(Task(rerun=True))


When used from an argument as follows:

.. code:: python

    # main.py
    class Task(gokart.TaskOnKart[str]):
        def run(self):
            self.dump('hello')

.. code:: sh

    python main.py Task --local-scheduler --rerun


``rerun`` parameter will look at the dependent tasks up to one level.

Example: Suppose we have a straight line pipeline composed of TaskA, TaskB and TaskC,  and TaskC is an endpoint of this pipeline. We also suppose that all the tasks have already been executed.

- TaskA(rerun=True)  ->  TaskB  ->  TaskC    # not rerunning
- TaskA  ->  TaskB(rerun=True)  ->  TaskC    # rerunning TaskB and TaskC

This is due to the way intermediate files are handled. ``rerun`` parameter is ``significant=False``, it does not affect the hash value. It is very important to understand this difference.


If you want to change the parameter of TaskA and rerun TaskB and TaskC, recommend adding an arbitrary parameter.

.. code:: python

    class TaskA(gokart.TaskOnKart):
        __version = luigi.IntParameter(default=1)

If the hash value of TaskA will change, the dependent tasks (in this case, TaskB and TaskC) will rerun.


Fix random seed
---------------

Every task has a parameter named :attr:`~gokart.task.TaskOnKart.fix_random_seed_methods` and :attr:`~gokart.task.TaskOnKart.fix_random_seed_value`. This can be used to fix the random seed.


.. code:: python

    import gokart
    import random
    import numpy
    import torch

    class Task(gokart.TaskOnKart[dict[str, Any]]):
        def run(self):
            x = [random.randint(0, 100) for _ in range(0, 10)]
            y = [np.random.randint(0, 100) for _ in range(0, 10)]
            z = [torch.randn(1).tolist()[0] for _ in range(0, 5)]
            self.dump({'random': x, 'numpy': y, 'torch': z})

    gokart.build(
        Task(
            fix_random_seed_methods=[
                "random.seed",
                "numpy.random.seed",
                "torch.random.manual_seed"],
            fix_random_seed_value=57))

::

    # //--- The output is as follows every time. ---
    # {'random': [65, 41, 61, 37, 55, 81, 48, 2, 94, 21],
    #   'numpy': [79, 86, 5, 22, 79, 98, 56, 40, 81, 37], 'torch': []}
    #   'torch': [0.14460121095180511, -0.11649507284164429,
    #            0.6928958296775818, -0.916053831577301, 0.7317505478858948]}

This will be useful for using Machine Learning Libraries.
