from logging import getLogger

import slack


logger = getLogger(__name__)


class ChannelListNotLoadedError(RuntimeError):
    pass


class ChannelNotFoundError(RuntimeError):
    pass


class FileNotUploadedError(RuntimeError):
    pass


class SlackAPI(object):
    def __init__(self, token, channel: str, to_user: str) -> None:
        self._client = slack.WebClient(token=token)
        self._channel_id = self._get_channel_id(channel)
        self._to_user = to_user if to_user == '' or to_user.startswith('@') else '@' + to_user

    def _get_channels(self, channels=[], cursor=None):
        params = {}
        if cursor:
            params['cursor'] = cursor
        response = self._client.api_call('channels.list', http_verb="GET", params=params)
        if not response['ok']:
            raise ChannelListNotLoadedError(f'Error while loading channels. The error reason is "{response["error"]}".')
        channels += response.get('channels', [])
        if not channels:
            raise ChannelListNotLoadedError('Channel list is empty.')
        if response['response_metadata']['next_cursor']:
            return self._get_channels(channels, response['response_metadata']['next_cursor'])
        else:
            return channels

    def _get_channel_id(self, channel_name):
        for channel in self._get_channels():
            if channel['name'] == channel_name:
                return channel['id']
        raise ChannelNotFoundError(f'Channel {channel_name} is not found in public channels.')

    def send_snippet(self, comment, title, content):
        request_body = dict(
            channels=self._channel_id,
            initial_comment=f'<{self._to_user}> {comment}' if self._to_user else comment,
            content=content,
            title=title)
        response = self._client.api_call('files.upload', data=request_body)
        if not response['ok']:
            raise FileNotUploadedError(f'Error while uploading file. The error reason is "{response["error"]}".')
