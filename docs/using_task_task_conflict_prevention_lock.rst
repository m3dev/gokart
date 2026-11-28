Task conflict prevention lock
=============================

If there is a possibility of multiple worker nodes executing the same task, the same ``run()`` may be executed on several nodes at once.
The run lock introduced in this page prevents it: the gokart worker takes a lock for a task with ``should_lock_run=True`` before it runs the task,
and a task whose lock is held by another worker waits until the lock is released (see :doc:`efficient_run_on_multi_workers`).

Outputs are not locked when they are dumped, loaded or removed. Every output is written to a temporary file first and moved (or uploaded) to its path when the write completes,
so a task never reads an output that is only partly written.
The locks that gokart used to take on ``dump()``, ``load()`` and ``remove()`` of outputs were removed (`#713 <https://github.com/m3dev/gokart/issues/713>`_).

Requires
--------

You need to install the ``redis`` client with ``pip install gokart[redis]``, and set up a `redis <https://redis.io/topics/quickstart>`_ server, for using this advanced feature.
When a Redis lock backend is configured (``url`` of ``[gokart_lock]``), a task with ``should_lock_run=True`` raises ``ImportError`` at construction if the ``redis`` client is not installed.
The run lock can also be stored on GCS or S3 instead of Redis (see :doc:`efficient_run_on_multi_workers`).


How to use
-----------


1. Set up a redis server at somewhere accessible from gokart/luigi jobs.

    e.g. Following script will run redis at your localhost.
    
    .. code:: bash

        $ redis-server

2. Set the redis server URL in the ``[gokart_lock]`` section of your config file.

    e.g.

    .. code::

        [gokart_lock]
        url=redis://localhost:6379
        timeout=180

    The URL has the form ``redis://host[:port][/db]``. The port defaults to 6379 and the db number to 0. Use ``rediss://`` to connect over TLS.
    Do not put a password in the URL: gokart reads it from the environment variable ``GOKART_REDIS_PASSWORD`` (no password is sent when it is unset or empty).

    .. code::

        [gokart_lock]
        url=rediss://redis.example.com:6380/2

    .. code::

        $ export GOKART_REDIS_PASSWORD=...

    ``timeout`` is the number of seconds after which a lock is released without extension (180 by default).

3. Set ``should_lock_run`` of the tasks that should not run on several nodes at once.

    .. code:: python

        class SampleTask(gokart.TaskOnKart):
            should_lock_run = True

    The run lock is taken by the gokart worker, i.e. when the task is run with ``gokart.build`` or ``gokart.run``.
    Setting only ``url`` does not lock anything.
