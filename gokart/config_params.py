import gokart
import luigi


class inherits_config_params:
    def __init__(self, config_class: luigi.Config):
        self.config_class: luigi.Config = config_class

    def __call__(self, task: gokart.TaskOnKart):
        config_class = self.config_class

        # wrap task to prevent task name from being changed
        @luigi.task._task_wraps(task)
        class Wrapped(task):
            @classmethod
            def get_param_values(cls, params, args, kwargs):
                for k, v in config_class().param_kwargs.items():
                    if hasattr(cls, k) and k not in kwargs:
                        kwargs[k] = v
                return super(Wrapped, cls).get_param_values(params, args, kwargs)

        return Wrapped
