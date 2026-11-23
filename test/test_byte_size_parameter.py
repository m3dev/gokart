import luigi
import pytest
from luigi.cmdline_parser import CmdlineParser

from gokart.parameter import ByteSizeParameter, OptionalByteSizeParameter


class _ByteSizeTask(luigi.Task):
    task_namespace = __name__
    size: ByteSizeParameter = ByteSizeParameter(default=1024)
    optional_size: OptionalByteSizeParameter = OptionalByteSizeParameter()


@pytest.mark.parametrize(
    ('text', 'expected'),
    [
        ('0', 0),
        ('1048576', 1048576),
        ('10KB', 10 * 1000),
        ('10MB', 10 * 1000**2),
        ('10GB', 10 * 1000**3),
        ('10KiB', 10 * 1024),
        ('10MiB', 10 * 1024**2),
        ('10GiB', 10 * 1024**3),
        ('256mib', 256 * 1024**2),
        ('256MIB', 256 * 1024**2),
        ('10 MB', 10 * 1000**2),
        (' 10  MiB ', 10 * 1024**2),
    ],
)
def test_parse(text, expected):
    assert ByteSizeParameter().parse(text) == expected


@pytest.mark.parametrize(
    'text',
    [
        # ambiguous: MB or MiB
        '10M',
        '10m',
        '10K',
        '10G',
        '10B',
        '1.5GiB',
        '1.5',
        '-1',
        '-10MB',
        'MB',
        '10 MB MB',
        # full-width digits
        '\uff11\uff10MB',
        '',
    ],
)
def test_parse_rejects(text):
    with pytest.raises(luigi.parameter.ParameterException):
        ByteSizeParameter().parse(text)


def test_serialize_round_trips_as_bytes():
    parameter = ByteSizeParameter()

    assert parameter.serialize(parameter.parse('10MiB')) == '10485760'
    assert parameter.parse(parameter.serialize(10485760)) == 10485760


def test_optional_is_none_by_default_and_for_an_empty_string():
    assert _ByteSizeTask().optional_size is None
    assert OptionalByteSizeParameter().parse('') is None
    assert OptionalByteSizeParameter().parse('10MB') == 10 * 1000**2


def test_cmdline():
    with CmdlineParser.global_instance([f'{__name__}._ByteSizeTask', '--size', '10MiB', '--optional-size', '1GB']) as cp:
        task = cp.get_task_obj()

    assert (task.size, task.optional_size) == (10 * 1024**2, 1000**3)


def test_config_file():
    config = luigi.configuration.get_config()
    section = f'{__name__}._ByteSizeTask'
    config.add_section(section)
    try:
        config.set(section, 'size', '256MiB')
        config.set(section, 'optional_size', '1 GB')

        task = _ByteSizeTask()
    finally:
        config.remove_section(section)

    assert (task.size, task.optional_size) == (256 * 1024**2, 1000**3)
