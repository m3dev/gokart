How to improve efficiency when running on multiple workers
===========================================================

If multiple worker nodes are running similar gokart pipelines in parallel, it is possible that the exact same task may be executed by multiple workers.
(For example, when training multiple machine learning models with different parameters, the feature creation task in the first stage is expected to be exactly the same.)

It is inefficient to execute the same task on each of multiple worker nodes, so we want to avoid this.
Here we introduce `should_lock_run` feature to improve this inefficiency.



Suppress run() of the same task with `should_lock_run`
------------------------------------------------------
When `gokart.TaskOnKart.should_lock_run` is set to True, the gokart worker takes a run lock before it runs the task.
If the same task is run()-ing by another worker, the task waits without failing, and other tasks that can be executed at that time are given priority.
After the other worker releases the lock, the task is retried. If the other worker has already finished the task, it is skipped because its output exists.

The lock is stored in Redis, so `redis_host` and `redis_port` must also be set.
The lock is extended while the task is running, and expires after `redis_timeout` seconds if the worker dies.

.. code:: python

    class SampleTask2(gokart.TaskOnKart):
        should_lock_run = True
        redis_host = 'localhost'
        redis_port = 6379

The run lock is taken by the gokart worker, i.e. when the task is run with `gokart.build` or `gokart.run`.
If a `should_lock_run` task is dumped outside the gokart worker, the run lock is not taken, so `dump()` falls back to the dump lock and emits a `UserWarning`.
This happens, for example, when the task is run with `luigi.build` or the luigi CLI, or when `dump()` is called directly in a unit test.
To get rid of the warning, run the task with `gokart.build` or `gokart.run`.


Additional Option
------------------

Skip completed tasks with `complete_check_at_run`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
By setting `gokart.TaskOnKart.complete_check_at_run` to True, the existence of the cache can be rechecked at run() time.

Default is True, but if the check takes too much time, you can set to False to inactivate the check.

.. code:: python

    class SampleTask1(gokart.TaskOnKart):
        complete_check_at_run = False
    
