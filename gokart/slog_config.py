import functools
import logging
import os

from pythonjsonlogger import json


class SlogConfig(object):
    """
    SlogConfig is for logging configuration, Utility-class.
    This class will read an environment variable named GOKART_LOGGER_FORMAT, then switch logging configuration.

    If "$GOKART_LOGGER_FORMAT=json", set logging configuration as structured logging.
    Else if "$GOKART_LOGGER_FORMAT=text", set logging configuration as plain text, normal logging.
    Otherwise, default setting is applied, structured logging.
    """

    default_base_format = '%(message)s %(lineno)d %(pathname)s %(asctime)s %(name)s %(levelname)s %(filename)s %(lineno)s %(message)s'
    default_date_format = '%Y/%m/%d %H:%M:%S'

    @staticmethod
    def switch_log_format(logger):
        """
        If $GOKART_LOGGER_FORMAT=json, set logging configuration as structured logging.
        On the other hand, set logging configuration as plain text.
        """
        logger_mode = os.environ.get('GOKART_LOGGER_FORMAT')
        if logger_mode and logger_mode.lower() not in {'text', 'json'}:
            raise Exception(f'Unknown logger format: {logger_mode}')
        # plain text mode, so nothing is applied,
        elif logger_mode and logger_mode.lower() == 'text':
            return logger
        else:
            if not isinstance(logger, logging.Logger):
                return logger
            # If logger configuration was loaded, skip configuration.
            if logger.hasHandlers():
                return logger
            current_logger, found_handler = logger, 0
            while current_logger:
                for handler in current_logger.handlers:
                    found_handler += 1
                    fmt = handler.formatter._fmt if handler.formatter and handler.formatter._fmt else SlogConfig.default_base_format
                    date_fmt = handler.formatter.datefmt if handler.formatter and handler.formatter.datefmt else SlogConfig.default_date_format
                    formatter = json.JsonFormatter(
                        fmt=fmt,
                        datefmt=date_fmt,
                    )
                    handler.setFormatter(formatter)

                if not current_logger.parent:
                    break

                current_logger = current_logger.parent

            if found_handler == 0:
                last_resort_handler = logging.lastResort if logging.lastResort is not None else logging.StreamHandler()
                fmt = (
                    last_resort_handler.formatter._fmt
                    if last_resort_handler.formatter and last_resort_handler.formatter._fmt
                    else SlogConfig.default_base_format
                )
                date_fmt = (
                    last_resort_handler.formatter.datefmt
                    if last_resort_handler.formatter and last_resort_handler.formatter.datefmt
                    else SlogConfig.default_date_format
                )
                formatter = json.JsonFormatter(
                    fmt=fmt,
                    datefmt=date_fmt,
                )
                last_resort_handler.setFormatter(formatter)
            return logger


# This is the decorator method for logging.getLogger.
def getLogger_decorator(func):
    @functools.wraps(func)
    def wrapper(name):
        logger = func(name)
        logger = SlogConfig.switch_log_format(logger)
        return logger

    return wrapper
