import logging
import os

from gokart.slog_config import SlogConfig

from pythonjsonlogger import json


class GokartLogger(logging.Logger):
    def addHandler(self, handler):
        super().addHandler(handler)
        logger_mode = os.environ.get('GOKART_LOGGER_FORMAT', 'json').lower()
        if logger_mode == 'json':
            fmt = SlogConfig._default_base_format
            date_fmt = SlogConfig._default_date_format
            formatter = json.JsonFormatter(fmt=fmt, datefmt=date_fmt)
            handler.setFormatter(formatter)

