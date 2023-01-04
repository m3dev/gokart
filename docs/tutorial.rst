Tutorial
========

Also please refer to :doc:`intro_to_gokart` section.


1, Make gokart project
----------------------

Create a project using `cookiecutter-gokart <https://github.com/m3dev/cookiecutter-gokart>`_.


.. code:: sh

    cookiecutter  https://github.com/m3dev/cookiecutter-gokart
    # project_name [project_name]: example
    # package_name [package_name]: gokart_example
    # python_version [3.7.0]:
    # author [your name]: m3dev
    # package_description [What's this project?]: gokart example
    # license [MIT License]:


You will have a directory tree like following:

.. code:: sh

    tree example/
    example/
    ├── Dockerfile
    ├── README.md
    ├── conf
    │   ├── logging.ini
    │   └── param.ini
    ├── gokart_example
    │   ├── __init__.py
    │   ├── model
    │   │   ├── __init__.py
    │   │   └── sample.py
    │   └── utils
    │       └── template.py
    ├── main.py
    ├── pyproject.toml
    └── test
        ├── __init__.py
        └── unit_test
            └── test_sample.py


2, Running sample task
----------------------

Let's run the first task.

.. code:: sh

    python main.py gokart_example.Sample --local-scheduler


The results are stored in resources directory.

.. code:: sh

    tree resources
    resources/
    ├── gokart_example
    │   └── model
    │       └── sample
    │           └── Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl
    └── log
        ├── module_versions
        │   └── Sample_cdf55a3d6c255d8c191f5f472da61f99.txt
        ├── processing_time
        │   └── Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl
        ├── random_seed
        │   └── Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl
        ├── task_log
        │   └── Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl
        └── task_params
            └── Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl


Please refer to :doc:`intro_to_gokart` for output

.. note::

    It is better to use poetry in terms of the module version. Please refer to `poetry document <https://python-poetry.org/docs/>`_

    .. code:: sh

        poetry lock
        poetry run python main.py gokart_example.Sample --local-scheduler

    If want to stabilize it further, please use docker.

    .. code:: sh

        docker build -t sample .
        docker run -it sample "python main.py gokart_example.Sample --local-scheduler"



3, Check result
---------------

Check the output.

.. code:: python

    with open('resources/gokart_example/model/sample/Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl', 'rb') as f:
        print(pickle.load(f))  # sample output


4, Run unittest
------------------

It is important to run unittest before and after modifying the code.

.. code:: sh

    python -m unittest discover -s ./test/unit_test/
    .
    ----------------------------------------------------------------------
    Ran 1 test in 0.001s

    OK

5, Create Task
--------------

Writing gokart-like tasks.
Modify ``example/gokart_example/model/sample.py`` as follows:


.. code:: python

    from logging import getLogger
    import gokart
    from gokart_example.utils.template import GokartTask
    logger = getLogger(__name__)


    class Sample(GokartTask):
        def run(self):
            self.dump('sample output')


    class StringToSplit(GokartTask):
        """Like the function to divide received data by spaces."""
        task = gokart.TaskInstanceParameter()

        def run(self):
            sample = self.load('task')
            self.dump(sample.split(' '))


    class Main(GokartTask):
        """Endpoint task."""
        def requires(self):
            return StringToSplit(task=Sample())


Added ``Main`` and ``StringToSplit``. ``StringToSplit`` is a function-like task that loads the result of an arbitrary task and splits it by spaces. ``Main`` is injecting ``Sample`` into ``StringToSplit``. It like Endpoint.

Let’s run the ``Main`` task.


.. code:: sh

    python main.py gokart_example.Main --local-scheduler


Please take a look at the logger output at this time.

::

    ===== Luigi Execution Summary =====

    Scheduled 3 tasks of which:
    * 1 complete ones were encountered:
        - 1 gokart_example.Sample(...)
    * 2 ran successfully:
        - 1 gokart_example.Main(...)
        - 1 gokart_example.StringToSplit(...)

    This progress looks :) because there were no failed tasks or missing dependencies

    ===== Luigi Execution Summary =====

As the log shows, ``Sample`` has been executed once, so the ``cache`` will be used.
The only things that worked were ``Main`` and ``StringToSplit``.


The output will look like the following, with the result in ``StringToSplit_b8a0ce6c972acbd77eae30f35da4307e.pkl``.

::

    tree resources/
    resources/
    ├── gokart_example
    │   └── model
    │       └── sample
    │           ├── Sample_cdf55a3d6c255d8c191f5f472da61f99.pkl
    │           └── StringToSplit_b8a0ce6c972acbd77eae30f35da4307e.pkl
    ...


.. code:: python

    with open('resources/gokart_example/model/sample/StringToSplit_b8a0ce6c972acbd77eae30f35da4307e.pkl', 'rb') as f:
        print(pickle.load(f))  # ['sample', 'output']


It was able to move the added task.


6, Rerun Task
-------------

Finally, let's rerun the task.
There are two ways to rerun a task.
Change the ``rerun parameter`` or ``parameters of the dependent tasks``.


``gokart.TaskOnKart`` can set ``rerun parameter`` for each task like following:

.. code:: python

    class Main(GokartTask):
        rerun=True

        def requires(self):
            return StringToSplit(task=Sample(rerun=True), rerun=True)

OR


Add new parameter on dependent tasks like following:

.. code:: python

    class Sample(GokartTask):
        version = luigi.IntParameter(default=1)

        def run(self):
            self.dump('sample output version {self.version}')


In both cases, all tasks will be rerun.
The difference is hash value given to output files.
The reurn parameter has no effect on the hash value.
So it will be rerun with the same hash value.

In the second method, ``version parameter`` is added to the ``Sample`` task.
This parameter will change the hash value of ``Sample`` and generate another output file.
And the dependent task, ``StringToSplit``, will also have a different hash value, and rerun.

Please refer to :doc:`task_settings` for details.

Please try rerunning task at hand:)


Feature
-------

This is the end of the gokart tutorial.
The tutorial is an introduction to some of the features.
There are still more useful features.

Please See :doc:`task_on_kart` section, :doc:`for_pandas` section and :doc:`task_parameters` section for more useful features of the task.

Have a good gokart life.
