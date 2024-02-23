Task conflict prevention lock
=========================

If there is a possibility of multiple worker nodes executing the same task, task cache conflict may happen.
Specifically, while node A is loading the cache of a task, node B may be writing to it.
This can lead to reading an inappropriate data and other unwanted behaviors.

The redis lock introduced in this page is a feature to prevent such cache collisions.

Requires
--------

You need to install `redis <https://redis.io/topics/quickstart>`_ for using this advanced feature.


How to use
-----------


1. Set up a redis server at somewhere accessible from gokart/luigi jobs.

    e.g. Following script will run redis at your localhost.
    
    .. code:: bash

        $ redis-server

2. Set redis server hostname and port number as parameters of gokart.TaskOnKart().

    You can set it by adding ``--redis-host=[your-redis-localhost] --redis-port=[redis-port-number]`` options to gokart python script.

    e.g. 

    .. code:: bash

        python main.py sample.SomeTask --local-scheduler --redis-host=localhost --redis-port=6379
    

    Alternatively, you may set parameters at config file.
    
    e.g.

    .. code::

        [TaskOnKart]
        redis_host=localhost
        redis_port=6379

3. Done
    
    With the above configuration, all tasks that inherits gokart.TaskOnKart will ask the redis server if any other node is not trying to access the same cache file at the same time whenever they access the file with dump or load.
