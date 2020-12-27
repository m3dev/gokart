from logging import getLogger

import slack_sdk

logger = getLogger(__name__)


class ChannelListNotLoadedError(RuntimeError):
    pass


class ChannelNotFoundError(RuntimeError):
    pass


class FileNotUploadedError(RuntimeError):
    pass


class SlackAPI(object):
    def __init__(self, token, channel: str, to_user: str) -> None:
        self._client = slack_sdk.WebClient(token=token)
        self._channel_id = self._get_channel_id(channel)
        self._to_user = to_user if to_user == '' or to_user.startswith('@') else '@' + to_user

    def _get_channel_id(self, channel_name):
        params = {'exclude_archived': True, 'limit': 100}
        try:
            for channels in self._client.conversations_list(params=params):
                if not channels:
                    raise ChannelListNotLoadedError('Channel list is empty.')
                for channel in channels.get('channels', []):
                    if channel['name'] == channel_name:
                        return channel['id']
            raise ChannelNotFoundError(f'Channel {channel_name} is not found in public channels.')
        except Exception as e:
            logger.warning(f'The job will start without slack notification: {e}')

    def send_snippet(self, comment, title, content):
        try:
            request_body = dict(channels=self._channel_id,
                                initial_comment=f'<{self._to_user}> {comment}' if self._to_user else comment,
                                content=content,
                                title=title)
            response = self._client.api_call('files.upload', data=request_body)
            if not response['ok']:
                raise FileNotUploadedError(f'Error while uploading file. The error reason is "{response["error"]}".')
        except Exception as e:
            logger.warning(f'Failed to send slack notification: {e}')
