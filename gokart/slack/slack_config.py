import luigi


class SlackConfig(luigi.Config):
    token_name = luigi.Parameter(default='SLACK_TOKEN', description='slack token environment variable.')
    channel = luigi.Parameter(default='', description='channel name for notification.')
    to_user = luigi.Parameter(default='', description='Optional; user name who is supposed to be mentioned.')
