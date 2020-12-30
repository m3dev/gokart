import os
import unittest
from unittest.mock import patch, MagicMock

import luigi
import luigi.mock

import gokart
from gokart.slack import SlackConfig
from gokart.run import _try_to_send_event_summary_to_slack


class _DummyTask(gokart.TaskOnKart):
    task_namespace = __name__
    param = luigi.Parameter()


class RunTest(unittest.TestCase):
    def setUp(self):
        luigi.configuration.LuigiConfigParser._instance = None
        luigi.mock.MockFileSystem().clear()
        os.environ.clear()

    @patch('sys.argv', new=['main', f'{__name__}._DummyTask', '--param', 'test', '--log-level=CRITICAL', '--local-scheduler'])
    def test_run(self):
        config_file_path = os.path.join(os.path.dirname(__name__), 'config', 'test_config.ini')
        luigi.configuration.LuigiConfigParser.add_config_path(config_file_path)
        os.environ.setdefault('test_param', 'test')
        with self.assertRaises(SystemExit) as exit_code:
            gokart.run()
        self.assertEqual(exit_code.exception.code, 0)

    @patch('sys.argv', new=['main', f'{__name__}._DummyTask', '--log-level=CRITICAL', '--local-scheduler'])
    def test_run_with_undefined_environ(self):
        config_file_path = os.path.join(os.path.dirname(__name__), 'config', 'test_config.ini')
        luigi.configuration.LuigiConfigParser.add_config_path(config_file_path)
        with self.assertRaises(luigi.parameter.MissingParameterException) as missing_parameter:
            gokart.run()

    @patch('sys.argv',
           new=[
               'main', '--tree-info-mode=simple', '--tree-info-output-path=tree.txt', f'{__name__}._DummyTask', '--param', 'test', '--log-level=CRITICAL',
               '--local-scheduler'
           ])
    @patch('luigi.LocalTarget', new=lambda path, **kwargs: luigi.mock.MockTarget(path, **kwargs))
    def test_run_tree_info(self):
        config_file_path = os.path.join(os.path.dirname(__name__), 'config', 'test_config.ini')
        luigi.configuration.LuigiConfigParser.add_config_path(config_file_path)
        os.environ.setdefault('test_param', 'test')
        tree_info = gokart.tree_info(mode='simple', output_path='tree.txt')
        with self.assertRaises(SystemExit):
            gokart.run()
        self.assertTrue(gokart.make_tree_info(_DummyTask(param='test')), tree_info.output().load())

    @patch('gokart.make_tree_info')
    def test_try_to_send_event_summary_to_slack(self, make_tree_info_mock: MagicMock):
        event_aggregator_mock = MagicMock()
        event_aggregator_mock.get_summury.return_value = f'{__name__}._DummyTask'
        event_aggregator_mock.get_event_list.return_value = f'{__name__}._DummyTask:[]'
        make_tree_info_mock.return_value = 'tree'

        def get_content(content: str, **kwargs):
            self.output = content

        slack_api_mock = MagicMock()
        slack_api_mock.send_snippet.side_effect = get_content

        cmdline_args = [f'{__name__}._DummyTask', '--param', 'test']
        with patch('gokart.slack.SlackConfig.send_tree_info', True):
            _try_to_send_event_summary_to_slack(slack_api_mock, event_aggregator_mock, cmdline_args)
        expects = os.linesep.join(['===== Event List ====', event_aggregator_mock.get_event_list(), os.linesep, '==== Tree Info ====', 'tree'])

        results = self.output
        self.assertEqual(expects, results)

        cmdline_args = [f'{__name__}._DummyTask', '--param', 'test']
        with patch('gokart.slack.SlackConfig.send_tree_info', False):
            _try_to_send_event_summary_to_slack(slack_api_mock, event_aggregator_mock, cmdline_args)
        expects = os.linesep.join([
            '===== Event List ====',
            event_aggregator_mock.get_event_list(), os.linesep, '==== Tree Info ====', 'Please add SlackConfig.send_tree_info to include tree-info'
        ])

        results = self.output
        self.assertEqual(expects, results)


if __name__ == '__main__':
    unittest.main()
