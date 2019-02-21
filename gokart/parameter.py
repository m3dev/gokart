import luigi
from luigi import task_register


class TaskInstanceParameter(luigi.Parameter):
    @staticmethod
    def _recursive(param_dict):
        params = dict(param_dict['params'])
        task_cls = task_register.Register.get_task_cls(param_dict['type'])
        for key, value in task_cls.get_params():
            if key in params:
                params[key] = value.parse(params[key])
        return task_cls(**params)

    def parse(self, s):
        if isinstance(s, str):
            s = luigi.DictParameter().parse(s)
        return self._recursive(s)

    def serialize(self, x):
        values = dict(type=x.get_task_family(), params=x.to_str_params(only_significant=True))
        return luigi.DictParameter().serialize(values)
