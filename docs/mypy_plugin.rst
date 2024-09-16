[Experimental] Mypy plugin
===========================

Mypy plugin provides type checking for gokart tasks using Mypy.
This feature is experimental.

How to use
--------------

Configure Mypy to use this plugin by adding the following to your ``mypy.ini`` file:

.. code:: ini

    [mypy]
    plugins = gokart.mypy:plugin

or by adding the following to your ``pyproject.toml`` file:

.. code:: toml

    [tool.mypy]
    plugins = ["gokart.mypy:plugin"]

Then, run Mypy as usual.

Examples
--------

For example the following code linted by Mypy:

.. code:: python

    import gokart
    import luigi


    class Foo(gokart.TaskOnKart):
        # NOTE: must all the parameters be annotated
        foo: int = luigi.IntParameter(default=1)
        bar: str = luigi.Parameter()



    Foo(foo=1, bar='2')   # OK
    Foo(foo='1') # NG because foo is not int and bar is missing


Mypy plugin checks TaskOnKart generic types.

.. code:: python

    class SampleTask(gokart.TaskOnKart):
        str_task: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()
        int_task: gokart.TaskOnKart[int] = gokart.TaskInstanceParameter()

        def requires(self):
            return dict(str=self.str_task, int=self.int_task)

        def run(self):
            s = self.load(self.str_task)  # This type is inferred with "str"
            i = self.load(self.int_task)  # This type is inferred with "int"

    SampleTask(
        str_task=StrTask(),  # mypy ok
        int_task=StrTask(),  # mypy error: Argument "int_task" to "StrTask" has incompatible type "StrTask"; expected "TaskOnKart[int]
    )
