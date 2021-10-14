import hashlib
import inspect
import os
import types
from importlib import import_module
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Set, Union

import luigi
import pandas as pd

import gokart
from gokart.file_processor import FileProcessor
from gokart.pandas_type_config import PandasTypeConfigMap
from gokart.parameter import ExplicitBoolParameter, ListTaskInstanceParameter, TaskInstanceParameter
from gokart.redis_lock import make_redis_params
from gokart.target import TargetOnKart

logger = getLogger(__name__)


class TaskOnKart(luigi.Task):
    """
    This is a wrapper class of luigi.Task.

    The key methods of a TaskOnKart are:

    * :py:meth:`make_target` - this makes output target with a relative file path.
    * :py:meth:`make_model_target` - this makes output target for models which generate multiple files to save.
    * :py:meth:`load` - this loads input files of this task.
    * :py:meth:`dump` - this save a object as output of this task.
    """

    workspace_directory = luigi.Parameter(default='./resources/',
                                          description='A directory to set outputs on. Please use a path starts with s3:// when you use s3.',
                                          significant=False)  # type: str
    local_temporary_directory = luigi.Parameter(default='./resources/tmp/', description='A directory to save temporary files.', significant=False)  # type: str
    rerun = luigi.BoolParameter(default=False, description='If this is true, this task will run even if all output files exist.', significant=False)
    strict_check = luigi.BoolParameter(default=False,
                                       description='If this is true, this task will not run only if all input and output files exist.',
                                       significant=False)
    modification_time_check = luigi.BoolParameter(default=False,
                                                  description='If this is true, this task will not run only if all input and output files exist,'
                                                  ' and all input files are modified before output file are modified.',
                                                  significant=False)
    serialized_task_definition_check = luigi.BoolParameter(default=False,
                                                           description='If this is true, even if all outputs are present,'
                                                           'this task will be executed if any changes have been made to the code.',
                                                           significant=False)
    delete_unnecessary_output_files = luigi.BoolParameter(default=False, description='If this is true, delete unnecessary output files.', significant=False)
    significant = luigi.BoolParameter(default=True,
                                      description='If this is false, this task is not treated as a part of dependent tasks for the unique id.',
                                      significant=False)
    fix_random_seed_methods = luigi.ListParameter(default=['random.seed', 'numpy.random.seed'], description='Fix random seed method list.', significant=False)
    fix_random_seed_value = luigi.IntParameter(default=None, description='Fix random seed method value.', significant=False)

    redis_host = luigi.OptionalParameter(default=None, description='Task lock check is deactivated, when None.', significant=False)
    redis_port = luigi.OptionalParameter(default=None, description='Task lock check is deactivated, when None.', significant=False)
    redis_timeout = luigi.IntParameter(default=180, description='Redis lock will be released after `redis_timeout` seconds', significant=False)
    redis_fail_on_collision: bool = luigi.BoolParameter(
        default=False,
        description='True for failing the task immediately when the cache is locked, instead of waiting for the lock to be released',
        significant=False)
    fail_on_empty_dump: bool = ExplicitBoolParameter(default=False, description='Fail when task dumps empty DF', significant=False)
    store_index_in_feather: bool = ExplicitBoolParameter(default=True,
                                                         description='Wether to store index when using feather as a output object.',
                                                         significant=False)

    cache_unique_id: bool = ExplicitBoolParameter(default=True, description='Cache unique id during runtime', significant=False)

    def __init__(self, *args, **kwargs):
        self._add_configuration(kwargs, 'TaskOnKart')
        # 'This parameter is dumped into "workspace_directory/log/task_log/" when this task finishes with success.'
        self.task_log = dict()
        self.task_unique_id = None
        super(TaskOnKart, self).__init__(*args, **kwargs)
        self._rerun_state = self.rerun
        self._lock_at_dump = True

    def output(self):
        return self.make_target()

    def requires(self):
        tasks = self.make_task_instance_dictionary()
        return tasks or []  # when tasks is empty dict, then this returns empty list.

    def make_task_instance_dictionary(self) -> Dict[str, 'TaskOnKart']:
        return {key: var for key, var in vars(self).items() if self.is_task_on_kart(var)}

    @staticmethod
    def is_task_on_kart(value):
        return isinstance(value, TaskOnKart) or (isinstance(value, tuple) and bool(value) and all([isinstance(v, TaskOnKart) for v in value]))

    @classmethod
    def _add_configuration(cls, kwargs, section):
        config = luigi.configuration.get_config()
        class_variables = dict(TaskOnKart.__dict__)
        class_variables.update(dict(cls.__dict__))
        if section not in config:
            return
        for key, value in dict(config[section]).items():
            if key not in kwargs and key in class_variables:
                kwargs[key] = class_variables[key].parse(value)

    def complete(self) -> bool:
        if self._rerun_state:
            for target in luigi.task.flatten(self.output()):
                target.remove()
            self._rerun_state = False
            return False

        is_completed = all([t.exists() for t in luigi.task.flatten(self.output())])

        if self.strict_check or self.modification_time_check:
            requirements = luigi.task.flatten(self.requires())
            inputs = luigi.task.flatten(self.input())
            is_completed = is_completed and all([task.complete() for task in requirements]) and all([i.exists() for i in inputs])

        if not self.modification_time_check or not is_completed or not self.input():
            return is_completed

        return self._check_modification_time()

    def _check_modification_time(self):
        common_path = set(t.path() for t in luigi.task.flatten(self.input())) & set(t.path() for t in luigi.task.flatten(self.output()))
        input_tasks = [t for t in luigi.task.flatten(self.input()) if t.path() not in common_path]
        output_tasks = [t for t in luigi.task.flatten(self.output()) if t.path() not in common_path]

        input_modification_time = max([target.last_modification_time() for target in input_tasks]) if input_tasks else None
        output_modification_time = min([target.last_modification_time() for target in output_tasks]) if output_tasks else None

        if input_modification_time is None or output_modification_time is None:
            return True

        # "=" must be required in the following statements, because some tasks use input targets as output targets.
        return input_modification_time <= output_modification_time

    def clone(self, cls=None, **kwargs):
        _SPECIAL_PARAMS = {'rerun', 'strict_check', 'modification_time_check'}
        if cls is None:
            cls = self.__class__

        new_k = {}
        for param_name, param_class in cls.get_params():
            if param_name in kwargs:
                new_k[param_name] = kwargs[param_name]
            elif hasattr(self, param_name) and (param_name not in _SPECIAL_PARAMS):
                new_k[param_name] = getattr(self, param_name)

        return cls(**new_k)

    def make_target(self, relative_file_path: str = None, use_unique_id: bool = True, processor: Optional[FileProcessor] = None) -> TargetOnKart:
        formatted_relative_file_path = relative_file_path if relative_file_path is not None else os.path.join(self.__module__.replace(".", "/"),
                                                                                                              f"{type(self).__name__}.pkl")
        file_path = os.path.join(self.workspace_directory, formatted_relative_file_path)
        unique_id = self.make_unique_id() if use_unique_id else None

        redis_params = make_redis_params(file_path=file_path,
                                         unique_id=unique_id,
                                         redis_host=self.redis_host,
                                         redis_port=self.redis_port,
                                         redis_timeout=self.redis_timeout,
                                         redis_fail_on_collision=self.redis_fail_on_collision)
        return gokart.target.make_target(file_path=file_path,
                                         unique_id=unique_id,
                                         processor=processor,
                                         redis_params=redis_params,
                                         store_index_in_feather=self.store_index_in_feather)

    def make_large_data_frame_target(self, relative_file_path: str = None, use_unique_id: bool = True, max_byte=int(2**26)) -> TargetOnKart:
        formatted_relative_file_path = relative_file_path if relative_file_path is not None else os.path.join(self.__module__.replace(".", "/"),
                                                                                                              f"{type(self).__name__}.zip")
        file_path = os.path.join(self.workspace_directory, formatted_relative_file_path)
        unique_id = self.make_unique_id() if use_unique_id else None
        redis_params = make_redis_params(file_path=file_path,
                                         unique_id=unique_id,
                                         redis_host=self.redis_host,
                                         redis_port=self.redis_port,
                                         redis_timeout=self.redis_timeout,
                                         redis_fail_on_collision=self.redis_fail_on_collision)
        return gokart.target.make_model_target(file_path=file_path,
                                               temporary_directory=self.local_temporary_directory,
                                               unique_id=unique_id,
                                               save_function=gokart.target.LargeDataFrameProcessor(max_byte=max_byte).save,
                                               load_function=gokart.target.LargeDataFrameProcessor.load,
                                               redis_params=redis_params)

    def make_model_target(self,
                          relative_file_path: str,
                          save_function: Callable[[Any, str], None],
                          load_function: Callable[[str], Any],
                          use_unique_id: bool = True):
        """
        Make target for models which generate multiple files in saving, e.g. gensim.Word2Vec, Tensorflow, and so on.

        :param relative_file_path: A file path to save.
        :param save_function: A function to save a model. This takes a model object and a file path.
        :param load_function: A function to load a model. This takes a file path and returns a model object.
        :param use_unique_id: If this is true, add an unique id to a file base name.
        """
        file_path = os.path.join(self.workspace_directory, relative_file_path)
        assert relative_file_path[-3:] == 'zip', f'extension must be zip, but {relative_file_path} is passed.'
        unique_id = self.make_unique_id() if use_unique_id else None
        redis_params = make_redis_params(file_path=file_path,
                                         unique_id=unique_id,
                                         redis_host=self.redis_host,
                                         redis_port=self.redis_port,
                                         redis_timeout=self.redis_timeout,
                                         redis_fail_on_collision=self.redis_fail_on_collision)
        return gokart.target.make_model_target(file_path=file_path,
                                               temporary_directory=self.local_temporary_directory,
                                               unique_id=unique_id,
                                               save_function=save_function,
                                               load_function=load_function,
                                               redis_params=redis_params)

    def load(self, target: Union[None, str, TargetOnKart] = None) -> Any:
        def _load(targets):
            if isinstance(targets, list) or isinstance(targets, tuple):
                return [_load(t) for t in targets]
            if isinstance(targets, dict):
                return {k: _load(t) for k, t in targets.items()}
            return targets.load()

        data = _load(self._get_input_targets(target))
        if target is None and isinstance(data, dict) and len(data) == 1:
            return list(data.values())[0]
        return data

    def load_generator(self, target: Union[None, str, TargetOnKart] = None) -> Any:
        def _load(targets):
            if isinstance(targets, list) or isinstance(targets, tuple):
                for t in targets:
                    yield from _load(t)
            elif isinstance(targets, dict):
                for k, t in targets.items():
                    yield from {k: _load(t)}
            else:
                yield targets.load()

        return _load(self._get_input_targets(target))

    def load_data_frame(self,
                        target: Union[None, str, TargetOnKart] = None,
                        required_columns: Optional[Set[str]] = None,
                        drop_columns: bool = False) -> pd.DataFrame:
        def _flatten_recursively(dfs):
            if isinstance(dfs, list):
                return pd.concat([_flatten_recursively(df) for df in dfs])
            else:
                return dfs

        dfs = self.load(target=target)
        if isinstance(dfs, dict) and len(dfs) == 1:
            dfs = list(dfs.values())[0]

        data = _flatten_recursively(dfs)

        required_columns = required_columns or set()
        if data.empty and len(data.index) == 0 and len(required_columns - set(data.columns)) > 0:
            return pd.DataFrame(columns=required_columns)
        assert required_columns.issubset(set(data.columns)), f'data must have columns {required_columns}, but actually have only {data.columns}.'
        if drop_columns:
            data = data[required_columns]
        return data

    def dump(self, obj, target: Union[None, str, TargetOnKart] = None) -> None:
        PandasTypeConfigMap().check(obj, task_namespace=self.task_namespace)
        if self.fail_on_empty_dump and isinstance(obj, pd.DataFrame):
            assert not obj.empty
        self._get_output_target(target).dump(obj, lock_at_dump=self._lock_at_dump)

    @staticmethod
    def get_code(target_class) -> Set[str]:
        def has_sourcecode(obj):
            return inspect.ismethod(obj) or inspect.isfunction(obj) or inspect.isframe(obj) or inspect.iscode(obj)

        return {inspect.getsource(t) for _, t in inspect.getmembers(target_class, has_sourcecode)}

    def get_own_code(self):
        gokart_codes = self.get_code(TaskOnKart)
        own_codes = self.get_code(self)
        return ''.join(sorted(list(own_codes - gokart_codes)))

    def make_unique_id(self):
        unique_id = self.task_unique_id or self._make_hash_id()
        if self.cache_unique_id:
            self.task_unique_id = unique_id
        return unique_id

    def _make_hash_id(self):
        def _to_str_params(task):
            if isinstance(task, TaskOnKart):
                return str(task.make_unique_id()) if task.significant else None
            return task.to_str_params(only_significant=True)

        dependencies = [_to_str_params(task) for task in luigi.task.flatten(self.requires())]
        dependencies = [d for d in dependencies if d is not None]
        dependencies.append(self.to_str_params(only_significant=True))
        dependencies.append(self.__class__.__name__)
        if self.serialized_task_definition_check:
            dependencies.append(self.get_own_code())
        return hashlib.md5(str(dependencies).encode()).hexdigest()

    def _get_input_targets(self, target: Union[None, str, TargetOnKart]) -> Union[TargetOnKart, List[TargetOnKart]]:
        if target is None:
            return self.input()
        if isinstance(target, str):
            return self.input()[target]
        return target

    def _get_output_target(self, target: Union[None, str, TargetOnKart]) -> TargetOnKart:
        if target is None:
            return self.output()
        if isinstance(target, str):
            return self.output()[target]
        return target

    def get_info(self, only_significant=False):
        params_str = {}
        params = dict(self.get_params())
        for param_name, param_value in self.param_kwargs.items():
            if (not only_significant) or params[param_name].significant:
                if type(params[param_name]) == gokart.TaskInstanceParameter:
                    params_str[param_name] = type(param_value).__name__ + '-' + param_value.make_unique_id()
                else:
                    params_str[param_name] = params[param_name].serialize(param_value)
        return params_str

    def _get_task_log_target(self):
        return self.make_target(f'log/task_log/{type(self).__name__}.pkl')

    def get_task_log(self) -> Dict:
        target = self._get_task_log_target()
        if self.task_log:
            return self.task_log
        if target.exists():
            return self.load(target)
        return dict()

    @luigi.Task.event_handler(luigi.Event.SUCCESS)
    def _dump_task_log(self):
        self.task_log['file_path'] = [target.path() for target in luigi.task.flatten(self.output())]
        self.dump(self.task_log, self._get_task_log_target())

    def _get_task_params_target(self):
        return self.make_target(f'log/task_params/{type(self).__name__}.pkl')

    def get_task_params(self) -> Dict:
        target = self._get_task_log_target()
        if target.exists():
            return self.load(target)
        return dict()

    @luigi.Task.event_handler(luigi.Event.START)
    def _set_random_seed(self):
        random_seed = self._get_random_seed()
        seed_methods = self.try_set_seed(self.fix_random_seed_methods, random_seed)
        self.dump({'seed': random_seed, 'seed_methods': seed_methods}, self._get_random_seeds_target())

    def _get_random_seeds_target(self):
        return self.make_target(f'log/random_seed/{type(self).__name__}.pkl')

    @staticmethod
    def try_set_seed(methods: List[str], random_seed: int) -> List[str]:
        success_methods = []
        for method_name in methods:
            try:
                for i, x in enumerate(method_name.split('.')):
                    if i == 0:
                        m = import_module(x)
                    else:
                        m = getattr(m, x)
                m(random_seed)
                success_methods.append(method_name)
            except ModuleNotFoundError:
                pass
            except AttributeError:
                pass
        return success_methods

    def _get_random_seed(self):
        if self.fix_random_seed_value:
            return self.fix_random_seed_value
        return int(self.make_unique_id(), 16) % (2**32 - 1)  # maximum numpy.random.seed

    @luigi.Task.event_handler(luigi.Event.START)
    def _dump_task_params(self):
        self.dump(self.to_str_params(only_significant=True), self._get_task_params_target())

    def _get_processing_time_target(self):
        return self.make_target(f'log/processing_time/{type(self).__name__}.pkl')

    def get_processing_time(self) -> str:
        target = self._get_processing_time_target()
        if target.exists():
            return self.load(target)
        return 'unknown'

    @luigi.Task.event_handler(luigi.Event.PROCESSING_TIME)
    def _dump_processing_time(self, processing_time):
        self.dump(processing_time, self._get_processing_time_target())

    @classmethod
    def restore(cls, unique_id):
        params = TaskOnKart().make_target(f'log/task_params/{cls.__name__}_{unique_id}.pkl', use_unique_id=False).load()
        return cls.from_str_params(params)

    @luigi.Task.event_handler(luigi.Event.FAILURE)
    def _log_unique_id(self, exception):
        logger.info(f'FAILURE:\n    task name={type(self).__name__}\n    unique id={self.make_unique_id()}')

    @luigi.Task.event_handler(luigi.Event.START)
    def _dump_module_versions(self):
        self.dump(self._get_module_versions(), self._get_module_versions_target())

    def _get_module_versions_target(self):
        return self.make_target(f'log/module_versions/{type(self).__name__}.txt')

    def _get_module_versions(self) -> str:
        module_versions = []
        for x in set([x.split('.')[0] for x in globals().keys() if isinstance(x, types.ModuleType) and '_' not in x]):
            module = import_module(x)
            if '__version__' in dir(module):
                if type(module.__version__) == str:
                    version = module.__version__.split(" ")[0]
                else:
                    version = '.'.join([str(v) for v in module.__version__])
                module_versions.append(f'{x}=={version}')
        return '\n'.join(module_versions)

    def __repr__(self):
        """
        Build a task representation like `MyTask(param1=1.5, param2='5', data_task=DataTask(id=35tyi))`
        """
        params = self.get_params()
        param_values = self.get_param_values(params, [], self.param_kwargs)

        # Build up task id
        repr_parts = []
        param_objs = dict(params)
        for param_name, param_value in param_values:
            param_obj = param_objs[param_name]
            if param_obj.significant:
                repr_parts.append(f'{param_name}={self._make_representation(param_obj, param_value)}')

        task_str = f'{self.get_task_family()}({", ".join(repr_parts)})'
        return task_str

    def _make_representation(self, param_obj: luigi.Parameter, param_value):
        if isinstance(param_obj, TaskInstanceParameter):
            return f'{param_value.get_task_family()}({param_value.make_unique_id()})'
        if isinstance(param_obj, ListTaskInstanceParameter):
            return f"[{', '.join(f'{v.get_task_family()}({v.make_unique_id()})' for v in param_value)}]"
        return param_obj.serialize(param_value)
