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

For example the following code linted by Mypy:

.. code:: python

    import gokart
    import luigi


    class Foo(gokart.TaskOnKart):
        # NOTE: must all the parameters be annotated
        foo: int = luigi.IntParameter(default=1)
        bar: str = luigi.IntParameter(default=2)



    Foo(foo=1, bar='2')   # OK
    Foo(foo='1') # NG because foo is not int and bar is missing
