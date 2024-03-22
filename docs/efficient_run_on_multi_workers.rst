How to improve efficiency when running on multiple workers
===========================================================

If multiple worker nodes are running similar gokart pipelines in parallel, it is possible that the exact same task may be executed by multiple workers.
(For example, when training multiple machine learning models with different parameters, the feature creation task in the first stage is expected to be exactly the same.)

It is inefficient to execute the same task on each of multiple worker nodes, so we want to avoid this.
Here we introduce `should_lock_run` feature to improve this inefficiency.



Suppress run() of the same task with `should_lock_run`
------------------------------------------------------
When `gokart.TaskOnKart.should_lock_run` is set to True, the task will fail if the same task is run()-ing by another worker.
By failing the task, other tasks that can be executed at that time are given priority.
After that, the failed task is automatically re-executed.

.. code:: python

    class SampleTask2(gokart.TaskOnKart):
        should_lock_run = True


Additional Option
------------------

Skip completed tasks with `complete_check_at_run`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
By setting `gokart.TaskOnKart.complete_check_at_run` to True, the existence of the cache can be rechecked at run() time.

Default is True, but if the check takes too much time, you can set to False to inactivate the check.

.. code:: python

    class SampleTask1(gokart.TaskOnKart):
        complete_check_at_run = False
    
