import inspect
from functools import partial
from typing import Any, Callable, Dict, Generic, TypeVar

import luigi

T = TypeVar('T')


class Depends(Generic[T]):
    def __init__(self, func: Callable[..., T]):
        self._func = func


def _resolve_dependencies(depends: Depends[T], kwargs: Dict[str, Any]) -> T:
    depends_func_signature = inspect.signature(depends._func)
    depends_func_params = depends_func_signature.parameters

    params = {}
    for param_name, param in depends_func_params.items():
        default = param.default
        annotation = param.annotation
        if isinstance(default, Depends):
            params[param_name] = _resolve_dependencies(default, kwargs)
        elif default is inspect.Parameter.empty and param_name in kwargs:
            params[param_name] = kwargs[param_name]
        elif default is not inspect.Parameter.empty:
            params[param_name] = default
        # NOTE: support luigi.Config as parameter of dependency functions
        elif issubclass(annotation, luigi.Config):
            params[param_name] = annotation()
        else:
            raise ValueError(f'Parameter {param_name} cannot be resolved.')
    return depends._func(**params)


def resolve_run_dependencies_wrapper(run: Callable, param_kwargs: Dict[str, Any]) -> Callable:
    """Resolve dependencies of `run` method of Task

    Dependencies are resolved by the following rules:

    * If the parameter has a default value, the default value is used.
    * If the parameter has a default value of Depends, the dependency is resolved recursively.
    * If the parameter name is defined in task parameters, the value is used.
    * If the parameter type is luigi.Config, the instance of the config is created and used.

    Args:
        run (Callable): callback to be wrapped
        param_kwargs (Dict[str, Any]): parameters of the task

    Returns:
        run (Callable): wrapped callback with dependencies resolved
    """
    run_signature = inspect.signature(run)
    params = run_signature.parameters

    dependencies: Dict[str, Any] = {}
    for param_name, param in params.items():
        default = param.default
        if default is inspect.Parameter.empty:
            raise ValueError(f'All parameters of `run` method must have Depends default. {param_name} has no default.')
        if not isinstance(default, Depends):
            raise ValueError(f'Only Depends is allowed `run` method parameter. {param_name} is not Depends.')
        dependencies[param_name] = _resolve_dependencies(default, param_kwargs)

    return partial(run, **dependencies)
