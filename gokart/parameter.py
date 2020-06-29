import json

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


class _TaskInstanceEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, luigi.Task):
            return TaskInstanceParameter().serialize(obj)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class ListTaskInstanceParameter(luigi.Parameter):
    def parse(self, s):
        return [TaskInstanceParameter().parse(x) for x in list(json.loads(s))]

    def serialize(self, x):
        return json.dumps(x, cls=_TaskInstanceEncoder)


class ExplicitBoolParameter(luigi.BoolParameter):
    def __init__(self, *args, **kwargs):
        luigi.Parameter.__init__(self, *args, **kwargs)

    def _parser_kwargs(self, *args, **kwargs):
        return luigi.Parameter._parser_kwargs(*args, *kwargs)
