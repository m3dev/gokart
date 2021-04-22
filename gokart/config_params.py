from typing import Dict, Optional

import luigi

import gokart


class inherits_config_params:
    def __init__(self, config_class: luigi.Config, parameter_alias: Optional[Dict[str, str]] = None):
        """
        Decorates task to inherit parameter value of `config_class`.

        * config_class: Inherit parameter value of this task to decorated task. Only parameter values exist in both tasks are inherited.
        * parameter_alias: Dictionary to map paramter names between config_class task and decorated task.
                           key: config_class's parameter name. value: decorated task's parameter name.
        """

        self._config_class: luigi.Config = config_class
        self._parameter_alias: Dict[str, str] = parameter_alias if parameter_alias is not None else {}

    def __call__(self, task: gokart.TaskOnKart):
        # wrap task to prevent task name from being changed
        @luigi.task._task_wraps(task)
        class Wrapped(task):
            @classmethod
            def get_param_values(cls, params, args, kwargs):
                for param_key, param_value in self._config_class().param_kwargs.items():
                    task_param_key = self._parameter_alias.get(param_key, param_key)

                    if hasattr(cls, task_param_key) and task_param_key not in kwargs:
                        kwargs[task_param_key] = param_value
                return super(Wrapped, cls).get_param_values(params, args, kwargs)

        return Wrapped
