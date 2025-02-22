import os
import unittest
import logging
from unittest.mock import patch
from pythonjsonlogger import json
from gokart import getLogger


class TestSlogConfig(unittest.TestCase):
    def setUp(self):
        logging.root.handlers = []
        logging.root.manager.loggerDict.clear()

    @patch.dict(os.environ, {"GOKART_LOGGER_FORMAT": "json"})
    def test_apply_slog_format_json(self):
        logger = getLogger("test_logger")
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        self.assertTrue(stream_handlers, "No stream_handlers defined")
        json_formatters = [h.formatter for h in stream_handlers if isinstance(h.formatter, json.JsonFormatter)]
        self.assertTrue(json_formatters, "No json_formatters defined")

    @patch.dict(os.environ, {"GOKART_LOGGER_FORMAT": "text"})
    def test_apply_slog_format_text(self):
        logger = getLogger("test_logger")
        stream_handlers = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
        self.assertTrue(stream_handlers, "No stream_handlers defined")
        json_formatters = [h.formatter for h in stream_handlers if isinstance(h.formatter, json.JsonFormatter)]
        self.assertFalse(json_formatters, "No json_formatters defined")

    @patch.dict(os.environ, {"GOKART_LOGGER_FORMAT": "invalid_value"})
    def test_apply_slog_format_invalid_env(self):
        with self.assertRaises(Exception) as context:
            _ = getLogger(__name__)
        self.assertEqual(str(context.exception), "Unknown logger format: invalid_value")

if __name__ == "__main__":
    unittest.main()
