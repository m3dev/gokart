Task cache collision lock
=========================

If there is a possibility of multiple worker nodes executing the same task, cache collision may happen.
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
    

Advanced: Using efficient task cache collision lock
-----------------------------------------

The cache lock introduced above will prevent cache collision.
However, above setting check collisions only when the task access the cache file (i.e. ``task.dump()``, ``task.load()`` and ``task.remove()``).
This will allow applications to run ``run()`` of same task at the same time, which is not time efficient.

Settings in this section will prevent running ``run()`` at the same time for efficiency.

If you try to run() the same task on multiple worker nodes at the same time, run() will fail on the second and subsequent node's tasks.
gokart will execute other unaffected tasks in the meantime. Since we have also set up the retry process, we will come back to the failed task later.
When it comes back, the first worker node has already completed run() and a cache has been created, so there is no need to run() on the second and subsequent nodes.
In this way, efficient distributed processing is made possible.


This setting must be done to each gokart task which you want to lock the ``run()```.

1. Set normal cache collision lock

    Follow the steps in ``How to use`` to set up cache collision lock.


2. Decorate ``run()`` with ``@RunWithLock`` 
    
    Decorate ``run()`` of your gokart tasks you want to lock with ``@RunWithLock``.

    .. code:: python

        from gokart.run_with_lock import RunWithLock

        class SomeTask(gokart.TaskOnKart):
            @RunWithLock
            def run(self):
                ...            


3. Set ``raise_task_lock_exception_on_collision`` parameter to true.

    This parameter will affect the behavior when the task's lock is taken by other applications or nodes.
    Setting ``raise_task_lock_exception_on_collision=True`` will make the task to be failed if the task's lock is taken by others.

    The parameter can be set by config file.
    
    .. code:: 

        [TaskOnKart]
        redis_host=localhost
        redis_port=6379
        raise_task_lock_exception_on_collision=true

4. Set retry parameters

    Set following parameters to retry when task failed.
    * ``retry_count``: the max number of retries
    * ``retry_delay``: this value is set in seconds

    .. code:: 

        [scheduler]
        retry_count=10000
        retry_delay=10

        [worker]
        keep_alive=true
