from __future__ import annotations

import luigi

import gokart


class inherits_config_params:
    def __init__(self, config_class: type[luigi.Config], parameter_alias: dict[str, str] | None = None):
        """
        Decorates task to inherit parameter value of `config_class`.

        * config_class: Inherit parameter value of this task to decorated task. Only parameter values exist in both tasks are inherited.
        * parameter_alias: Dictionary to map paramter names between config_class task and decorated task.
                           key: config_class's parameter name. value: decorated task's parameter name.
        """

        self._config_class: type[luigi.Config] = config_class
        self._parameter_alias: dict[str, str] = parameter_alias if parameter_alias is not None else {}

    def __call__(self, task_class: type[gokart.TaskOnKart]):
        # wrap task to prevent task name from being changed
        @luigi.task._task_wraps(task_class)
        class Wrapped(task_class):  # type: ignore
            @classmethod
            def get_param_values(cls, params, args, kwargs):
                for param_key, param_value in self._config_class().param_kwargs.items():
                    task_param_key = self._parameter_alias.get(param_key, param_key)

                    if hasattr(cls, task_param_key) and task_param_key not in kwargs:
                        kwargs[task_param_key] = param_value
                return super().get_param_values(params, args, kwargs)

        return Wrapped
