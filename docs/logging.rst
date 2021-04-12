Logging
=======

How to set up a common logger for gokart.


Core settings
-------------

Please write a configuration file similar to the following:

::

    # base.ini
    [core]
    logging_conf_file=./conf/logging.ini

.. code:: python

    import gokart
    gokart.add_config('base.ini')


Logger ini file
---------------

It is the same as a general logging.ini file.

::

    [loggers]
    keys=root,luigi,luigi-interface,gokart,gokart.file_processor

    [handlers]
    keys=stderrHandler

    [formatters]
    keys=simpleFormatter

    [logger_root]
    level=INFO
    handlers=stderrHandler

    [logger_gokart]
    level=INFO
    handlers=stderrHandler
    qualname=gokart
    propagate=0

    [logger_luigi]
    level=INFO
    handlers=stderrHandler
    qualname=luigi
    propagate=0

    [logger_luigi-interface]
    level=INFO
    handlers=stderrHandler
    qualname=luigi-interface
    propagate=0

    [logger_gokart.file_processor]
    level=CRITICAL
    handlers=stderrHandler
    qualname=gokart.file_processor

    [handler_stderrHandler]
    class=StreamHandler
    formatter=simpleFormatter
    args=(sys.stdout,)

    [formatter_simpleFormatter]
    format=[%(asctime)s][%(name)s][%(levelname)s](%(filename)s:%(lineno)s) %(message)s
    datefmt=%Y/%m/%d %H:%M:%S

Please refer to `Python logging documentation <https://docs.python.org/3/library/logging.config.html>`_
