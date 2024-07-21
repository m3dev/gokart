import luigi

import gokart


class IntFoo(gokart.TaskOnKart[int]):
    # NOTE: mypy shows attr-defined error for the following lines, so we need to ignore it.
    a: str = luigi.Parameter()

    def run(self):
        self.dump(1)


class Bar(gokart.TaskOnKart[str]):
    def __init__(self, str_foo: gokart.TaskOnKartProtocol[str]):
        self.str_foo = str_foo

    def run(self):
        self.dump('bar')



bar = Bar(str_foo=IntFoo(a='1'))


