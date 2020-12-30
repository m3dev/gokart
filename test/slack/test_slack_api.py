import unittest
from logging import getLogger
from unittest import mock
from unittest.mock import MagicMock

from testfixtures import LogCapture
from slack_sdk.web.slack_response import SlackResponse
from slack_sdk import WebClient

import gokart.slack

logger = getLogger(__name__)


def _slack_response(token, data):
    return SlackResponse(client=WebClient(token=token),
                         http_verb="POST",
                         api_url="http://localhost:3000/api.test",
                         req_args={},
                         data=data,
                         headers={},
                         status_code=200)


class TestSlackAPI(unittest.TestCase):
    @mock.patch('gokart.slack.slack_api.slack_sdk.WebClient')
    def test_initialization_with_invalid_token(self, patch):
        def _conversations_list(params={}):
            return _slack_response(token='invalid', data={'ok': False, 'error': 'error_reason'})

        mock_client = MagicMock()
        mock_client.conversations_list = MagicMock(side_effect=_conversations_list)
        patch.return_value = mock_client

        with LogCapture() as l:
            gokart.slack.SlackAPI(token='invalid', channel='test', to_user='test user')
            l.check(('gokart.slack.slack_api', 'WARNING', 'The job will start without slack notification: Channel test is not found in public channels.'))

    @mock.patch('gokart.slack.slack_api.slack_sdk.WebClient')
    def test_invalid_channel(self, patch):
        def _conversations_list(params={}):
            return _slack_response(token='valid',
                                   data={
                                       'ok': True,
                                       'channels': [{
                                           'name': 'valid',
                                           'id': 'valid_id'
                                       }],
                                       'response_metadata': {
                                           'next_cursor': ''
                                       }
                                   })

        mock_client = MagicMock()
        mock_client.conversations_list = MagicMock(side_effect=_conversations_list)
        patch.return_value = mock_client

        with LogCapture() as l:
            gokart.slack.SlackAPI(token='valid', channel='invalid_channel', to_user='test user')
            l.check(('gokart.slack.slack_api', 'WARNING',
                     'The job will start without slack notification: Channel invalid_channel is not found in public channels.'))

    @mock.patch('gokart.slack.slack_api.slack_sdk.WebClient')
    def test_send_snippet_with_invalid_token(self, patch):
        def _conversations_list(params={}):
            return _slack_response(token='valid',
                                   data={
                                       'ok': True,
                                       'channels': [{
                                           'name': 'valid',
                                           'id': 'valid_id'
                                       }],
                                       'response_metadata': {
                                           'next_cursor': ''
                                       }
                                   })

        def _api_call(method, data={}):
            assert method == 'files.upload'
            return {'ok': False, 'error': 'error_reason'}

        mock_client = MagicMock()
        mock_client.conversations_list = MagicMock(side_effect=_conversations_list)
        mock_client.api_call = MagicMock(side_effect=_api_call)
        patch.return_value = mock_client

        with LogCapture() as l:
            api = gokart.slack.SlackAPI(token='valid', channel='valid', to_user='test user')
            api.send_snippet(comment='test', title='title', content='content')
            l.check(('gokart.slack.slack_api', 'WARNING', 'Failed to send slack notification: Error while uploading file. The error reason is "error_reason".'))

    @mock.patch('gokart.slack.slack_api.slack_sdk.WebClient')
    def test_send(self, patch):
        def _conversations_list(params={}):
            return _slack_response(token='valid',
                                   data={
                                       'ok': True,
                                       'channels': [{
                                           'name': 'valid',
                                           'id': 'valid_id'
                                       }],
                                       'response_metadata': {
                                           'next_cursor': ''
                                       }
                                   })

        def _api_call(method, data={}):
            assert method == 'files.upload'
            return {'ok': False, 'error': 'error_reason'}

        mock_client = MagicMock()
        mock_client.conversations_list = MagicMock(side_effect=_conversations_list)
        mock_client.api_call = MagicMock(side_effect=_api_call)
        patch.return_value = mock_client

        api = gokart.slack.SlackAPI(token='valid', channel='valid', to_user='test user')
        api.send_snippet(comment='test', title='title', content='content')


if __name__ == '__main__':
    unittest.main()
