=================
Task Parameters
=================

Luigi Parameter
================

We can set parameters for tasks.
Also please refer to :doc:`task_settings` section.

.. code:: python

    class Task(gokart.TaskOnKart):
        param_a = luigi.Parameter()
        param_c = luigi.ListParameter()
        param_d = luigi.IntParameter(default=1)

Please refer to `luigi document <https://luigi.readthedocs.io/en/stable/api/luigi.parameter.html>`_ for a list of parameter types.


Gokart Parameter
================

There are also parameters provided by gokart.

- gokart.TaskInstanceParameter
- gokart.ListTaskInstanceParameter
- gokart.ExplicitBoolParameter


gokart.TaskInstanceParameter
--------------------------------

The :func:`~gokart.parameter.TaskInstanceParameter` executes a task using the results of a task as dynamic parameters.


.. code:: python

    class TaskA(gokart.TaskOnKart[str]):
        def run(self):
            self.dump('Hello')


    class TaskB(gokart.TaskOnKart[str]):
        require_task = gokart.TaskInstanceParameter()

        def requires(self):
            return self.require_task

        def run(self):
            task_a = self.load()
            self.dump(','.join([task_a, 'world']))

    task = TaskB(require_task=TaskA())
    print(gokart.build(task))  # Hello,world


Helps to create a pipeline.


gokart.ListTaskInstanceParameter
-------------------------------------

The :func:`~gokart.parameter.ListTaskInstanceParameter` is list of TaskInstanceParameter.


gokart.ExplicitBoolParameter
-----------------------------------

The :func:`~gokart.parameter.ExplicitBoolParameter` is parameter for explicitly specified value.

``luigi.BoolParameter`` already has "explicit parsing" feature, but also still has implicit behavior like follows.

::

    $ python main.py Task --param
    # param will be set as True
    $ python main.py Task
    # param will be set as False

``ExplicitBoolParameter`` solves these problems on parameters from command line.
