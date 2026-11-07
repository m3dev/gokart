[Deprecated] Mypy plugin
=========================

Mypy plugin provides type checking for gokart tasks using Mypy.

.. warning::

    The mypy plugin is deprecated and will be removed in a future release.

    Annotate parameters with the parameter type instead.
    Type checkers that support ``dataclass_transform``, such as mypy and pyright, check this style without any plugin.
    Note that the plugin does not support this style, so remove the plugin from your configuration when you migrate.

    .. code:: python

        import gokart
        import luigi


        class Foo(gokart.TaskOnKart[str]):
            foo: luigi.IntParameter = luigi.IntParameter(default=1)
            bar: luigi.StrParameter = luigi.StrParameter()
            task: gokart.TaskInstanceParameter[gokart.TaskOnKart[int]] = gokart.TaskInstanceParameter()


How to use
--------------

Configure Mypy to use this plugin by adding the following to your ``mypy.ini`` file:

.. code:: ini

    [mypy]
    plugins = gokart.mypy:plugin

or by adding the following to your ``pyproject.toml`` file:

.. code:: toml

    [tool.mypy]
    plugins = ["gokart.mypy"]

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

Configurations (only pyproject.toml)
-----------------------------------

You can configure the Mypy plugin using the ``pyproject.toml`` file.
The following options are available:

.. code:: toml

    [tool.gokart-mypy]
    # If true, Mypy will raise an error if a task is missing required parameters.
    # This configuration causes an error when the parameters set by `luigi.Config()`
    # Default: false
    disallow_missing_parameters = true
