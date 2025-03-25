import io
import json
import logging
import os
import unittest
from unittest.mock import patch

from gokart import getLogger


class TestSlogConfig(unittest.TestCase):
    @patch.dict(os.environ, {'GOKART_LOGGER_FORMAT': 'json'})
    def test_switch_log_format_json(self):
        logger_name = 'test_json_logger'
        test_log_message = 'This should be structured log message.'

        logger = getLogger(logger_name)
        log_stream = io.StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger.addHandler(log_handler)

        logger.warning(test_log_message)

        log_contents = log_stream.getvalue().strip()
        json_log_contents = json.loads(log_contents)

        self.assertEqual(json_log_contents['message'], test_log_message)
        self.assertEqual(json_log_contents['name'], logger_name)

    @patch.dict(os.environ, {'GOKART_LOGGER_FORMAT': 'text'})
    def test_switch_log_format_text(self):
        logger_name = 'test_text_logger'
        test_log_message = 'This should be structured log message.'

        logger = getLogger(logger_name)
        log_stream = io.StringIO()
        log_handler = logging.StreamHandler(log_stream)
        logger.addHandler(log_handler)

        logger.warning(test_log_message)

        log_contents = log_stream.getvalue().strip()

        self.assertEqual(log_contents, test_log_message)

    @patch.dict(os.environ, {'GOKART_LOGGER_FORMAT': 'invalid_value'})
    def test_switch_log_format_invalid_env(self):
        with self.assertRaises(Exception) as context:
            _ = getLogger(__name__)
        self.assertEqual(str(context.exception), 'Unknown logger format: invalid_value')


if __name__ == '__main__':
    unittest.main()
