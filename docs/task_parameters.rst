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


gokart.SerializableParameter
----------------------------

The :func:`~gokart.parameter.SerializableParameter` is a parameter for any object that can be serialized and deserialized.
This parameter is particularly useful when you want to pass a complex object or a set of parameters to a task.

The object must implement the following methods:

- ``gokart_serialize``: Serialize the object to a string. This serialized string must uniquely identify the object to enable task caching.
  Note that it is not required for deserialization.
- ``gokart_deserialize``: Deserialize the object from a string, typically used for CLI arguments.

Example
^^^^^^^

.. code-block:: python

    import json
    from dataclasses import dataclass

    import gokart

    @dataclass(frozen=True)
    class Config:
        foo: int
        # The `bar` field does not affect the result of the task.
        # Similar to `luigi.Parameter(significant=False)`.
        bar: str

        def gokart_serialize(self) -> str:
            # Serialize only the `foo` field since `bar` is irrelevant for caching.
            return json.dumps({'foo': self.foo})

        @classmethod
        def gokart_deserialize(cls, s: str) -> 'Config':
            # Deserialize the object from the provided string.
            return cls(**json.loads(s))

    class DummyTask(gokart.TaskOnKart):
        config: Config = gokart.SerializableParameter(object_type=Config)

        def run(self):
            # Save the `config` object as part of the task result.
            self.dump(self.config)
