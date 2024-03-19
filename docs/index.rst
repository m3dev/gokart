.. gokart documentation master file, created by
   sphinx-quickstart on Fri Jan 11 07:59:25 2019.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to gokart's documentation!
==================================

Useful links: `GitHub <https://github.com/m3dev/gokart>`_ | `cookiecutter gokart <https://github.com/m3dev/cookiecutter-gokart>`_

`Gokart <https://github.com/m3dev/gokart>`_ is a wrapper of the data pipeline library `luigi <https://github.com/spotify/luigi>`_. Gokart solves "**reproducibility**", "**task dependencies**", "**constraints of good code**", and "**ease of use**" for Machine Learning Pipeline.


Good thing about gokart
-----------------------

Here are some good things about gokart.

- The following data for each Task is stored separately in a pkl file with hash value
    - task output data
    - imported all module versions
    - task processing time
    - random seed in task
    - displayed log
    - all parameters set as class variables in the task
- If change parameter of Task, rerun spontaneously.
    - The above file will be generated with a different hash value
    - The hash value of dependent task will also change and both will be rerun
- Support GCS or S3
- The above output is exchanged between tasks as an intermediate file, which is memory-friendly
- pandas.DataFrame type and column checking during I/O
- Directory structure of saved files is automatically determined from structure of script
- Seeds for numpy and random are automatically fixed
- Can code while adhering to SOLID principles as much as possible
- Tasks are locked via redis even if they run in parallel

**These are all functions baptized for creating Machine Learning batches. Provides an excellent environment for reproducibility and team development.**



Getting started
-----------------

.. toctree::
   :maxdepth: 2

   intro_to_gokart
   tutorial

User Guide
-----------------

.. toctree::
   :maxdepth: 2

   task_on_kart
   task_parameters
   setting_task_parameters
   task_settings
   task_information
   logging
   slack_notification
   using_task_task_conflict_prevention_lock
   efficient_run_on_multi_workers
   for_pandas

API References
--------------
.. toctree::
   :maxdepth: 2

   gokart


Indices and tables
-------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
