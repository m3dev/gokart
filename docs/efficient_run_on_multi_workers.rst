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

The lock is stored in Redis, so `url` of the `[gokart_lock]` config must also be set, and the Redis client must be installed with ``pip install gokart[redis]``.
The lock is extended while the task is running, and expires after `timeout` seconds if the worker dies.

.. code:: python

    class SampleTask2(gokart.TaskOnKart):
        should_lock_run = True

.. code:: ini

    [gokart_lock]
    url=redis://localhost:6379
    timeout=180

`redis_host`, `redis_port` and `redis_timeout` of `gokart.TaskOnKart` still work, but they are deprecated: when set, they take precedence over `[gokart_lock]` and emit a `FutureWarning`.

Object storage (GCS) backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If your pipeline already stores its outputs on GCS, the run lock can live there too, so you do not have to operate Redis just for it.

.. code:: ini

    [gokart_lock]
    url=gs://my-bucket/gokart-locks
    timeout=180
    skew_margin=30

The lock is one object per task, ``<prefix>/<lock_key>.lock``, written with GCS request preconditions (``ifGenerationMatch``).
A waiter treats the lock as expired when the server-side last write time + `timeout` + `skew_margin` seconds has passed, or when it has seen the same version unchanged for `timeout` seconds on its own monotonic clock.
Only one waiter wins a takeover. `skew_margin` (30 seconds by default) absorbs clock skew between workers.

Notes:

- Only the run lock moves to object storage. By design, the dump / load / remove locks (see :doc:`using_task_task_conflict_prevention_lock`) are **not taken** when the url is an object storage URL, because they need Redis. A task that holds the run lock does not need them under the gokart worker. For any other dump / load / remove, gokart emits a ``UserWarning`` to tell you that no lock is taken. If you rely on the dump lock, keep the url on Redis.
- Objects left behind by dead workers are not removed automatically. Add a bucket lifecycle rule (for example, delete after 1 day) as a safety net. Lifecycle works in whole days, so it does not replace `timeout`.
- Use the Standard storage class for the bucket, because other classes charge for early deletion.
- GCS allows about one write per second to the same object. The 10-second heartbeat is well within that.
- ``s3://`` is not supported yet.

The run lock is taken by the gokart worker, i.e. when the task is run with `gokart.build` or `gokart.run`.
If a `should_lock_run` task is dumped outside the gokart worker, the run lock is not taken, so `dump()` falls back to the dump lock and emits a `UserWarning`.
This happens, for example, when the task is run with `luigi.build` or the luigi CLI, or when `dump()` is called directly in a unit test.
To get rid of the warning, run the task with `gokart.build` or `gokart.run`.


Additional Option
------------------

Skip completed tasks with `complete_check_at_run`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
By setting `gokart.TaskOnKart.complete_check_at_run` to True, the existence of the cache can be rechecked at run() time.

The check is performed by the gokart worker (used by `gokart.build` and `gokart.run`) just before run(), when either `complete_check_at_run` of the task or `task_completion_check_at_run` of the `[gokart_worker]` config is True.
Both default to True, so if the check takes too much time, set both to False to inactivate the check.
Note that the check is not performed when the task is run without the gokart worker (e.g. `luigi.build` or the `luigi` command).

.. code:: python

    class SampleTask1(gokart.TaskOnKart):
        complete_check_at_run = False

.. code:: ini

    [gokart_worker]
    task_completion_check_at_run=false
    
