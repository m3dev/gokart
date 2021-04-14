from typing import Dict, Optional
import luigi

import gokart


class inherits_config_params:
    def __init__(self, config_class: luigi.Config, param_config2task: Optional[Dict[str, str]] = None):
        self._config_class: luigi.Config = config_class
        self._param_config2task: Dict[str, str] = param_config2task if param_config2task is not None else {}

    def __call__(self, task: gokart.TaskOnKart):
        # wrap task to prevent task name from being changed
        @luigi.task._task_wraps(task)
        class Wrapped(task):
            @classmethod
            def get_param_values(cls, params, args, kwargs):
                for param_key, param_value in self._config_class().param_kwargs.items():
                    task_param_key = self._param_config2task.get(param_key, param_key)

                    if hasattr(cls, task_param_key) and task_param_key not in kwargs:
                        kwargs[task_param_key] = param_value
                return super(Wrapped, cls).get_param_values(params, args, kwargs)

        return Wrapped
