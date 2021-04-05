Tutorial
========

Also please refer to :doc:`intro_to_gokart` section.


1, Make gokart project
----------------------

Create a project using `cookiecutter-gokart <https://github.com/m3dev/cookiecutter-gokart>`_.

.. code:: sh

    cookiecutter  https://github.com/m3dev/cookiecutter-gokart
    project_name [project_name]: example
    package_name [package_name]: gokart_example
    python_version [3.6]:
    author [your name]: m3dev
    package_description [What's this project?]: gokart example
    license [MIT License]:

You will have a directory tree like following:

.. code:: sh

    tree example
    example
    ├── README.md
    ├── conf
    │   ├── logging.ini
    │   └── param.ini
    ├── gokart_example
    │   ├── __init__.py
    │   └── model
    │       ├── __init__.py
    │       └── sample.py
    ├── main.py
    ├── setup.py
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

    tree resource
    resource
    ├── data
    │   └── sample_cdf55a3d6c255d8c191f5f472da61f99.pkl
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

3, Check result
---------------

Check the output.

.. code:: python

    with open('resource/data/sample_cdf55a3d6c255d8c191f5f472da61f99.pkl', 'rb') as f:
        print(pickle.load(f))  # sample output


4, Running unittet
------------------

It is important to do unittest before modifying the code.

.. code:: sh

    python -m unittest discover -s ./test/unit_test/
    .
    ----------------------------------------------------------------------
    Ran 1 test in 0.001s

    OK

5, Create a Task
----------------

[TBD] # TODO: We need to fix the cookiecutter gokart first.
