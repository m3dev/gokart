1. Task cache collision lock
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Requires
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You need to install (redis)[https://redis.io/topics/quickstart] for this
advanced function.

Description
^^^^^^^^^^^

Task lock is implemented to prevent task cache collision. (Originally,
task cache collision may occur when same task with same parameters run
at different applications parallelly.)

1. Set up a redis server at somewhere accessible from gokart/luigi jobs.

    Following will run redis at your localhost.
    
    .. code:: bash

        $ redis-server

2. Set redis server hostname and port number as parameters to gokart.TaskOnKart().

    You can set it by adding ``--redis-host=[your-redis-localhost] --redis-port=[redis-port-number]`` options to gokart python script.

    e.g. 

    .. code:: bash

        python main.py sample.SomeTask –local-scheduler –redis-host=localhost –redis-port=6379
    

    Alternatively, you may set parameters at config file.
    
    .. code::

        [TaskOnKart]
        redis_host=localhost
        redis_port=6379

2. Using efficient task cache collision lock
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


Description
^^^^^^^^^^^

Above task lock will prevent cache collision. However, above setting check collisions only when the task access the cache file (i.e. ``task.dump()``, ``task.load()`` and ``task.remove()``). This will allow applications to run ``run()`` of same task at the same time, which
is not efficent.

Settings in this section will prevent running ``run()`` at the same time for efficiency.

1. Set normal cache collision lock Set cache collision lock following ``1. Task cache collision lock``.

2. Decorate ``run()`` with ``@RunWithLock`` Decorate ``run()`` of yourt gokart tasks which you want to lock with ``@RunWithLock``.

    .. code:: python

        from gokart.run_with_lock import RunWithLock

        @RunWithLock
        class SomeTask(gokart.TaskOnKart):
            def run(self):            


3. Set ``redis_fail_on_collision`` parameter to true. This parameter will affect the behavior when the task’s lock is taken by other application. By setting ``redis_fail_on_collision=True``, task will be failed if the task’s lock is taken by other application. The locked task will be skipped and other independent task will be done first. If ``redis_fail_on_collision=False``, it will wait until the lock of other application is released.

    The parameter can be set by config file.
    
    .. code:: 

        [TaskOnKart]
        redis_host=localhost
        redis_port=6379
        redis_fail_on_collision=true

4. Set retry parameters. Set following parameters to retry when task
   failed. Values of ``retry_count`` and ``retry_delay``\ can be set to
   any value depends on your situation.

    ::

        [scheduler]
        retry_count=10000
        retry_delay=10

        [worker]
        keep_alive=true