import luigi
from luigi import task_register


class TaskInstanceParameter(luigi.Parameter):
    def parse(self, s):
        values = luigi.DictParameter().parse(s)
        return task_register.Register.get_task_cls(values['type'])(**values['params'])

    def serialize(self, x):
        values = dict(type=x.get_task_family(), params=x.to_str_params(only_significant=True))
        return luigi.DictParameter().serialize(values)
