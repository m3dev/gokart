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

`redis_host`, `redis_port` and `redis_timeout` of `gokart.TaskOnKart` still work, but they are deprecated: when set, they take precedence over `[gokart_lock]` and emit a `FutureWarning`. They will be removed in gokart 2.0.0 (`#685 <https://github.com/m3dev/gokart/issues/685>`_).

Object storage (GCS / S3) backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
If your pipeline already stores its outputs on GCS or S3, the run lock can live there too, so you do not have to operate Redis just for it.

.. code:: ini

    [gokart_lock]
    url=gs://my-bucket/gokart-locks
    timeout=180
    skew_margin=30

Use ``s3://my-bucket/gokart-locks`` for S3. Do not put a comment after the value: luigi's config parser keeps it as part of the url, and gokart rejects a url that contains whitespace.

The lock is one object per task, ``<prefix>/<lock_key>.lock``, written with conditional requests: ``ifGenerationMatch`` on GCS, ``If-None-Match: *`` / ``If-Match: <ETag>`` on S3.
A waiter treats the lock as expired when the server-side last write time + `timeout` + `skew_margin` seconds has passed, or when it has seen the same version unchanged for `timeout` seconds on its own monotonic clock.
Only one waiter wins a takeover. `skew_margin` (30 seconds by default) absorbs clock skew between workers.

Notes:

- The service account needs ``storage.objects.get``, ``storage.objects.create`` and ``storage.objects.delete`` on the bucket. Overwriting an existing object (every heartbeat) also requires ``delete``, so ``roles/storage.objectCreator`` is not enough; ``roles/storage.objectUser`` works.
- On S3, the credentials need ``s3:GetObject``, ``s3:PutObject``, ``s3:DeleteObject`` and ``s3:ListBucket`` on the bucket. Without ``s3:ListBucket``, S3 answers a read of a missing key with 403 instead of 404, so every task with ``should_lock_run=True`` fails before it runs. A conditional delete with an ETag also needs ``s3:GetObject``.
- Objects are left behind whenever ``release()`` never runs, for example after SIGKILL or an OOM kill of the worker. Add a lifecycle rule as a safety net, **scoped to the lock prefix**. A bucket-wide age rule would also delete your pipeline outputs when the bucket is shared with them. For GCS:

  .. code:: json

      {
        "rule": [
          {"action": {"type": "Delete"}, "condition": {"age": 1, "matchesPrefix": ["gokart-locks/"]}},
          {"action": {"type": "Delete"}, "condition": {"daysSinceNoncurrentTime": 1, "matchesPrefix": ["gokart-locks/"]}}
        ]
      }

  For S3:

  .. code:: json

      {"Rules": [{"ID": "gokart-locks", "Status": "Enabled",
        "Filter": {"Prefix": "gokart-locks/"},
        "Expiration": {"Days": 1},
        "NoncurrentVersionExpiration": {"NoncurrentDays": 1}}]}

  One day is safe because the age of an object counts from the write of its current generation, and every heartbeat writes a new one, so a live lock never reaches it. The noncurrent-version rule only matters with object versioning (see below). Lifecycle works in whole days, so it does not replace `timeout`.
- With object versioning, every heartbeat (every 10 seconds) leaves a noncurrent version, so keep a lifecycle rule for noncurrent versions (the second rule above on GCS, ``NoncurrentVersionExpiration`` on S3, about 360 versions per hour per running task) or use a bucket without versioning. Soft delete also keeps deleted lock objects for its retention period.
- `timeout` must be greater than 50 seconds (the 10-second heartbeat interval plus the 10-second request timeout and the 30-second retry deadline of a lock request), and `skew_margin` must not be negative; otherwise a task with ``should_lock_run=True`` fails at construction. The 180 seconds by default is fine.
- Use the Standard storage class for the bucket, because other classes charge for early deletion.
- GCS allows about one write per second to the same object. The 10-second heartbeat is well within that.
- A failure to release the lock (for example a missing ``storage.objects.delete`` permission) is only logged as a warning, and the lock expires after `timeout`.
- With ``aws_role_arn`` in the luigi ``[s3]`` section, the credentials come from a single ``sts:AssumeRole`` call that luigi does not refresh, and every lock does its own AssumeRole when it is created. A task with ``should_lock_run=True`` that outlives the session (one hour by default) gets ``ExpiredToken`` on its heartbeats, and its lock is taken over after `timeout`. The S3 targets have the same luigi limitation. Use credentials that refresh themselves (for example an instance or workload role) for long-running tasks, or extend the role session duration.
- S3 needs ``botocore>=1.35.69`` for conditional writes, and ``pip install gokart[s3]``. Conditional deletes are available on general purpose buckets since 2025-09.
- To use an S3-compatible store such as MinIO or R2, set ``endpoint_url`` of the ``[S3Config]`` section. It is shared by the S3 targets and the S3 run lock.

  .. code:: ini

      [gokart_lock]
      url=s3://my-bucket/gokart-locks

      [S3Config]
      endpoint_url=http://localhost:9000

- The S3 targets and the S3 run lock read the same options: ``endpoint_url`` of ``[S3Config]``, and the ``[s3]`` section of luigi (``endpoint_url``, ``region_name``, ``aws_session_token``, ``aws_role_arn`` / ``aws_role_session_name``, ...). If both set ``endpoint_url``, the one in ``[S3Config]`` wins.
- Plain S3-compatible stores vary, and conditional PUT and conditional DELETE are separate features. Cloudflare R2 documents conditional headers for PutObject, CopyObject and multipart uploads, but not for DeleteObject. MinIO is not verified either. gokart raises an error when the store rejects the conditional headers, but it cannot detect a store that silently ignores them. On such a store the lock does not exclude anything, and the same task can run concurrently on several workers: a holder whose lock was taken over after it expired would delete the new holder's lock with an unconditional delete, letting a third worker in. Check your store before relying on the lock.

The run lock is taken by the gokart worker, i.e. when the task is run with `gokart.build` or `gokart.run`.
If a `should_lock_run` task is dumped outside the gokart worker, the run lock is not taken, so no lock is taken at all, and `dump()` emits a `UserWarning`.
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
    

Reduce repeated `exists()` calls with `cache_task_completion`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Before a task is run, the gokart worker checks the completion of the task when it is added, of every dependency just before the task runs, and of the task itself again (see above).
With remote storage such as GCS or S3, each check is an API call, so a build with many tasks and dependencies makes many duplicated `exists()` calls.

Setting `cache_task_completion` of the `[gokart_worker]` config to true makes the worker remember the tasks that were found complete when they were added, and reuse the result instead of calling `exists()` again.
The worker also records every task it has finished running, and when `check_complete_on_run` is false (the default) such a task is treated as complete without calling `exists()`. If its `run()` ends without dumping an output, the tasks that depend on it fail with a `FileNotFoundError` in `load()` instead of `Unfulfilled dependency at run time`.
For example, for a task with 10 dependencies that are already complete, the number of `exists()` calls drops from 22 to 12.
The check at run() time controlled by `task_completion_check_at_run` also uses the cache, but only `True` is cached and the task was incomplete when it was added, so it still detects a task that was finished elsewhere.

.. code:: ini

    [gokart_worker]
    cache_task_completion=true

The cache assumes that a task which was complete stays complete during the build.
Avoid it when outputs may be deleted while the build is running: a dependency whose output disappeared after it was added is still treated as complete, so the worker neither reports it as missing nor re-adds it to be rescheduled (up to `max_reschedules`), and the task that depends on it fails when it loads the output.
