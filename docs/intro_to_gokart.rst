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

    class Example(gokart.TaskOnKart):
        def run(self):
            self.dump('Hellow, world!')

    task = Example()
    output = gokart.build(task)
    print(output)


``gokart.build`` return the result of dump by ``gokart.TaskOnKart``. The example will output the following.


.. code:: python

    Hellow, world!


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
        print(pickle.load(f))  # Hellow, world!


That will be given hash value depending on the parameter of the task. This means that if you change the parameter of the task, the hash value will change, and change output file. This is very useful when changing parameters and experimenting. Please refer to :doc:`setting_task_parameters` for task parameters. Also see :doc:`task_output` for information on how to return this output destination.
# TODO: add task_output


In addition, the following files are automatically saved as ``log``.

- ``module_versions``: The versions of all modules that were imported when the script was executed. For reproducibility.
- ``processing_time``: The execution time of the task.
- ``random_seed``: This is random seed of python and numpy. For reproducibility in Machine Learning. please refer to :doc:`fix_random_seed`.
- ``task_log``: This is the output of the task logger.
- ``task_params``: This is task's parameters. Please refer to :doc:`setting_task_parameters`.

# TODO: add fix_random_seed


How to running task
-------------------

gokart has ``run`` and ``build`` methods for running task. Each has a different purpose.

- ``gokart.run``: uses arguments on the shell. return retcode.
- ``gokart.build``: uses inline code on jupyter notebook, IPython, and more. return task output.


Next :doc:`tutorial` section is explained by ``gokart.build``. If want to make a batch that runs in shell, please refer to :doc:`gokart_run` section.
# TODO: add run section

