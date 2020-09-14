# gokart Slack notification

## Prerequisites

prepare following environmental variables:
```
export SLACK_TOKEN=xoxb-your-token    // should use token starts with "xoxb-" (bot token is preferable)
export SLACK_CHANNEL=channel-name     // not "#channel-name", just "channel-name"
```

A Slack bot token can obtain from [here](https://api.slack.com/apps).

A bot token needs following scopes:

- `channels:read`
- `chat:write`
- `files:write`

More about scopes are [here](https://api.slack.com/scopes).

## Implement Slack notification

Write following codes pass arguments to your gokart workflow.

```python
    cmdline_args = sys.argv[1:]
    if 'SLACK_CHANNEL' in os.environ:
        cmdline_args.append(f'--SlackConfig-channel={os.environ["SLACK_CHANNEL"]}')
    if 'SLACK_TO_USER' in os.environ:
        cmdline_args.append(f'--SlackConfig-to-user={os.environ["SLACK_TO_USER"]}')
    gokart.run(cmdline_args)
```
