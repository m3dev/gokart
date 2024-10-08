import luigi

import gokart
from test.config import TEST_CONFIG_INI


class FooConfig(luigi.Config):
    name: str = luigi.Parameter()


class Foo:
    def __init__(self, name: str, version: str) -> None:
        self.name = name
        self.version = version


class Bar:
    def __init__(self, foo: Foo) -> None:
        self.foo = foo


def build_foo(config: FooConfig, version: str) -> Foo:
    """`build_foo` function depends on `FooConfig` and `version` parameters.
    `version` parameter is passed from `DummyTask` in the resolving dependencies process.
    """
    return Foo(name=config.name, version=version)


def build_bar(foo: Foo = gokart.Depends(build_foo)) -> Bar:
    """NOTE: `build_bar` depends on the result of `build_foo` function."""
    return Bar(foo=foo)


class DummyTask(gokart.TaskOnKart[None]):
    task_namespace = "aaaaaaaaa"
    version: str = luigi.Parameter()

    def run(self, foo: Foo = gokart.Depends(build_foo), bar: Bar = gokart.Depends(build_bar)):
        assert isinstance(foo, Foo)
        assert isinstance(bar, Bar)
        assert foo.name == 'foo'
        assert foo.version == self.version
        assert bar.foo.name == 'foo'
        self.dump(None)


def test_success():
    gokart.utils.add_config(str(TEST_CONFIG_INI))
    gokart.build(DummyTask(version='1.0.0'))
