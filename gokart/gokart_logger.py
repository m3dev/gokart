import logging
import os

from pythonjsonlogger import json

from gokart.slog_config import SlogConfig


class GokartLogger(logging.Logger):
    def addHandler(self, handler):
        super().addHandler(handler)
        logger_mode = os.environ.get('GOKART_LOGGER_FORMAT', 'json').lower()
        if logger_mode == 'json':
            fmt = handler.formatter._fmt if handler.formatter and handler.formatter._fmt else SlogConfig.default_base_format
            date_fmt = handler.formatter.datefmt if handler.formatter and handler.formatter.datefmt else SlogConfig.default_date_format
            formatter = json.JsonFormatter(
                fmt=fmt,
                datefmt=date_fmt,
            )
            handler.setFormatter(formatter)
