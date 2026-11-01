Slack notification
=========================

.. deprecated::
   ``gokart.slack`` (Slack notification) is deprecated and will be removed in a future major release.
   A ``DeprecationWarning`` is emitted when the notification is activated.
   Please use Luigi event handlers instead (see `Migration`_ below).

Prerequisites
-------------

Prepare following environmental variables:

.. code:: sh

   export SLACK_TOKEN=xoxb-your-token    // should use token starts with "xoxb-" (bot token is preferable)
   export SLACK_CHANNEL=channel-name     // not "#channel-name", just "channel-name"


A Slack bot token can obtain from `slack app document <https://api.slack.com/apps>`_.

A bot token needs following scopes:

- `channels:read`
- `chat:write`
- `files:write`

More about scopes are `slack scopes document <https://api.slack.com/scopes>`_.

Implement Slack notification
----------------------------

Write following codes pass arguments to your gokart workflow.

.. code:: python

    cmdline_args = sys.argv[1:]
    if 'SLACK_CHANNEL' in os.environ:
        cmdline_args.append(f'--SlackConfig-channel={os.environ["SLACK_CHANNEL"]}')
    if 'SLACK_TO_USER' in os.environ:
        cmdline_args.append(f'--SlackConfig-to-user={os.environ["SLACK_TO_USER"]}')
    gokart.run(cmdline_args)


Migration
---------

Luigi's event handlers can send notifications to Slack (or any other service) without gokart's help.
The following example posts a message when a task fails:

.. code:: python

   import os

   import luigi
   from slack_sdk import WebClient

   client = WebClient(token=os.environ['SLACK_TOKEN'])


   @luigi.Task.event_handler(luigi.Event.FAILURE)
   def notify_failure(task, exception):
       client.chat_postMessage(
           channel=os.environ['SLACK_CHANNEL'],
           text=f'Task {task} failed: {exception}',
       )
