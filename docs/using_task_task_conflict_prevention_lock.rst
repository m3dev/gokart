Task conflict prevention lock
=========================

If there is a possibility of multiple worker nodes executing the same task, task cache conflict may happen.
Specifically, while node A is loading the cache of a task, node B may be writing to it.
This can lead to reading an inappropriate data and other unwanted behaviors.

The redis lock introduced in this page is a feature to prevent such cache collisions.

Requires
--------

You need to install the ``redis`` client with ``pip install gokart[redis]``, and set up a `redis <https://redis.io/topics/quickstart>`_ server, for using this advanced feature.


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

    Only ``redis://host[:port]`` is supported for now; the port defaults to 6379.
    ``timeout`` is the number of seconds after which a lock is released without extension (180 by default).

    The deprecated ``redis_host``, ``redis_port`` and ``redis_timeout`` parameters of gokart.TaskOnKart (e.g. ``--redis-host=localhost --redis-port=6379``) still work.
    When they are set, they take precedence over ``[gokart_lock]`` and emit a ``DeprecationWarning``. They will be removed in the next major release.

3. Done
    
    With the above configuration, all tasks that inherits gokart.TaskOnKart will ask the redis server if any other node is not trying to access the same cache file at the same time whenever they access the file with dump or load.
