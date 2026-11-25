Task conflict prevention lock
=============================

If there is a possibility of multiple worker nodes executing the same task, task cache conflict may happen.
Specifically, while node A is loading the cache of a task, node B may be writing to it.
This can lead to reading an inappropriate data and other unwanted behaviors.

The redis lock introduced in this page is a feature to prevent such cache collisions.

Requires
--------

You need to install the ``redis`` client with ``pip install gokart[redis]``, and set up a `redis <https://redis.io/topics/quickstart>`_ server, for using this advanced feature.
When a Redis lock backend is configured (``url`` of ``[gokart_lock]``, or the deprecated ``redis_host`` and ``redis_port``), a task raises ``ImportError`` at construction if the ``redis`` client is not installed, even if the task never takes a lock.


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
    The password, db number and TLS setting apply only to the server given by ``url``, not to a host set with the deprecated ``redis_host``.

    .. code::

        [gokart_lock]
        url=rediss://redis.example.com:6380/2

    .. code::

        $ export GOKART_REDIS_PASSWORD=...

    ``timeout`` is the number of seconds after which a lock is released without extension (180 by default).

    The deprecated ``redis_host``, ``redis_port`` and ``redis_timeout`` parameters of gokart.TaskOnKart (e.g. ``--redis-host=localhost --redis-port=6379``) still work.
    When they are set, they take precedence over ``[gokart_lock]`` and emit a ``FutureWarning``. They will be removed in gokart 2.0.0 (`#685 <https://github.com/m3dev/gokart/issues/685>`_).
    ``redis_host`` and ``redis_port`` replace the whole endpoint (both are required, and the db number, TLS and password of ``url`` are not used), while ``redis_timeout`` falls back to ``timeout``.
    The replacement of ``--redis-host`` / ``--redis-port`` on the command line is ``--gokart-lock-url=redis://localhost:6379``.
    If only one of ``redis_host`` and ``redis_port`` is set, a ``FutureWarning`` says that ``url`` is ignored and no Redis lock is taken.
    Custom targets that call ``make_task_lock_params(redis_host=self.redis_host, ...)`` themselves do not see ``[gokart_lock]``; create them with ``self.make_target`` / ``self.make_model_target`` instead.
    ``TaskOnKart.redis_timeout`` is ``None`` unless it is set explicitly (it used to default to 180), so code that reads it should use ``timeout`` of ``[gokart_lock]`` instead.

3. Done
    
    With the above configuration, all tasks that inherits gokart.TaskOnKart will ask the redis server if any other node is not trying to access the same cache file at the same time whenever they access the file with dump or load.
