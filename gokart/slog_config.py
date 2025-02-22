import functools
import logging
import os
import sys

from pythonjsonlogger import json


class SlogConfig(object):
    """
    LoggerConfig is for logging configuration, Utility-class.
    This class will read an environment variable named GOKART_LOGGER_FORMAT, then switch logging configuration.

    If "$GOKART_LOGGER_FORMAT=json", set logging configuration as structured logging.
    Else if "$GOKART_LOGGER_FORMAT=text", set logging configuration as plain text, normal logging.
    Otherwise, default setting is applied, structured logging.
    """

    _default_base_format = '%(message)s %(lineno)d %(pathname)s %(asctime)s %(name)s %(levelname)s %(filename)s %(lineno)s %(message)s'
    _default_date_format = '%Y/%m/%d %H:%M:%S'

    @staticmethod
    def apply_slog_format(logger):
        """
        If $GOKART_LOGGER_FORMAT=json, set logging configuration as structured logging.
        On the other hand,
        """
        logger_mode = os.environ.get('GOKART_LOGGER_FORMAT')
        if not logger_mode or logger_mode.lower() == 'json':
            if not isinstance(logger, logging.Logger):
                return
            if not logger.handlers:
                stream_handler = logging.StreamHandler(sys.stdout)
                logger.addHandler(stream_handler)

            for handler in logger.handlers:
                if isinstance(handler, logging.StreamHandler):
                    fmt = handler.formatter._fmt if handler.formatter and handler.formatter._fmt else SlogConfig._default_base_format
                    date_fmt = handler.formatter.datefmt if handler.formatter and handler.formatter.datefmt else SlogConfig._default_date_format

                    formatter = json.JsonFormatter(
                        fmt=fmt,
                        datefmt=date_fmt,
                    )
                    handler.setFormatter(formatter)
            return logger
        # plain text mode, so nothing is applied.
        elif logger_mode.lower() == 'text':
            if not logger.handlers:
                handler = logging.StreamHandler(sys.stdout)
                logger.addHandler(handler)
            return logger
        else:
            raise Exception(f'Unknown logger format: {logger_mode}')


# This is the decorator method for logging.getLogger.
def getLogger_decorator(func):
    @functools.wraps(func)
    def wrapper(name):
        logger = func(name)
        logger = SlogConfig.apply_slog_format(logger)
        return logger

    return wrapper
