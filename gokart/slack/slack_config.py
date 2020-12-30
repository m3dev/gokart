import luigi


class SlackConfig(luigi.Config):
    token_name = luigi.Parameter(default='SLACK_TOKEN', description='slack token environment variable.')
    channel = luigi.Parameter(default='', significant=False, description='channel name for notification.')
    to_user = luigi.Parameter(default='', significant=False, description='Optional; user name who is supposed to be mentioned.')
    send_tree_info = luigi.BoolParameter(default=False,
                                         significant=False,
                                         description='When this option is true, the dependency tree of tasks is included in send message.'
                                         'It is recommended to set false to this option when notification takes long time.')
