from functools import partial

import luigi


class RunWithLock:
    def __init__(self, func):
        self._func = func

    def __call__(self, instance):
        instance._lock_at_dump = False
        output_list = luigi.task.flatten(instance.output())
        return self._run_with_lock(partial(self._func, self=instance), output_list)

    def __get__(self, instance, owner_class):
        return partial(self.__call__, instance)

    @classmethod
    def _run_with_lock(cls, func, output_list: list):
        if len(output_list) == 0:
            return func()

        output = output_list.pop()
        wrapped_func = output.wrap_with_lock(func)
        return cls._run_with_lock(func=wrapped_func, output_list=output_list)
