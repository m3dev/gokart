import unittest
from logging import getLogger
from unittest import mock
from unittest.mock import MagicMock

import gokart.slack

logger = getLogger(__name__)


class TestSlackAPI(unittest.TestCase):
    @mock.patch('gokart.slack.slack_api.slack.WebClient')
    def test_initialization_with_invalid_token(self, patch):
        def _channels_list(method):
            assert method == 'channels.list'
            return {'ok': False, 'error': 'error_reason'}

        mock_client = MagicMock()
        mock_client.api_call = MagicMock(side_effect=_channels_list)
        patch.return_value = mock_client

        with self.assertRaises(gokart.slack.slack_api.ChannelListNotLoadedError):
            gokart.slack.SlackAPI(token='invalid', channel='test', to_user='test user')

    @mock.patch('gokart.slack.slack_api.slack.WebClient')
    def test_invalid_channel(self, patch):
        def _channels_list(method):
            assert method == 'channels.list'
            return {'ok': True, 'channels': [{'name': 'valid', 'id': 'valid_id'}]}

        mock_client = MagicMock()
        mock_client.api_call = MagicMock(side_effect=_channels_list)
        patch.return_value = mock_client

        with self.assertRaises(gokart.slack.slack_api.ChannelNotFoundError):
            gokart.slack.SlackAPI(token='valid', channel='invalid_channel', to_user='test user')

    @mock.patch('gokart.slack.slack_api.slack.WebClient')
    def test_send_snippet_with_invalid_token(self, patch):
        def _api_call(*args, **kwargs):
            if args[0] == 'channels.list':
                return {'ok': True, 'channels': [{'name': 'valid', 'id': 'valid_id'}]}
            if args[0] == 'files.upload':
                return {'ok': False, 'error': 'error_reason'}
            assert False

        mock_client = MagicMock()
        mock_client.api_call = MagicMock(side_effect=_api_call)
        patch.return_value = mock_client

        with self.assertRaises(gokart.slack.slack_api.FileNotUploadedError):
            api = gokart.slack.SlackAPI(token='valid', channel='valid', to_user='test user')
            api.send_snippet(comment='test', title='title', content='content')

    @mock.patch('gokart.slack.slack_api.slack.WebClient')
    def test_send(self, patch):
        def _api_call(*args, **kwargs):
            if args[0] == 'channels.list':
                return {'ok': True, 'channels': [{'name': 'valid', 'id': 'valid_id'}]}
            if args[0] == 'files.upload':
                return {'ok': True}
            assert False

        mock_client = MagicMock()
        mock_client.api_call = MagicMock(side_effect=_api_call)
        patch.return_value = mock_client

        api = gokart.slack.SlackAPI(token='valid', channel='valid', to_user='test user')
        api.send_snippet(comment='test', title='title', content='content')


if __name__ == '__main__':
    unittest.main()
