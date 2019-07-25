import hashlib
import os
from logging import getLogger
from typing import Union, List, Any, Callable, Set, Optional, Dict

import luigi
import pandas as pd

import gokart
from gokart.file_processor import FileProcessor
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

    workspace_directory = luigi.Parameter(
        default='./resources/', description='A directory to set outputs on. Please use a path starts with s3:// when you use s3.',
        significant=False)  # type: str
    local_temporary_directory = luigi.Parameter(default='./resources/tmp/', description='A directory to save temporary files.', significant=False)  # type: str
    rerun = luigi.BoolParameter(default=False, description='If this is true, this task will run even if all output files exist.', significant=False)
    strict_check = luigi.BoolParameter(
        default=False, description='If this is true, this task will not run only if all input and output files exits.', significant=False)
    modification_time_check = luigi.BoolParameter(
        default=False,
        description='If this is true, this task will not run only if all input and output files exits,'
        ' and all input files are modified before output file are modified.',
        significant=False)
    delete_unnecessary_output_files = luigi.BoolParameter(default=False, description='If this is true, delete unnecessary output files.', significant=False)

    def __init__(self, *args, **kwargs):
        self._add_configuration(kwargs, self.get_task_family())
        self._add_configuration(kwargs, 'TaskOnKart')
        # 'This parameter is dumped into "workspace_directory/log/task_log/" when this task finishes with success.'
        self.task_log = dict()
        self.task_unique_id = None
        super(TaskOnKart, self).__init__(*args, **kwargs)
        self._rerun_state = self.rerun

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

        input_modification_time = max([target.last_modification_time() for target in luigi.task.flatten(self.input())])
        output_modification_time = min([target.last_modification_time() for target in luigi.task.flatten(self.output())])
        # "=" must be required in the following statements, because some tasks use input targets as output targets.
        return input_modification_time <= output_modification_time

    def clone(self, cls=None, **kwargs):
        if cls is None:
            cls = self.__class__

        new_k = {}
        for param_name, param_class in cls.get_params():
            if param_name in {'rerun', 'strict_check', 'modification_time_check'}:
                continue

            if param_name in kwargs:
                new_k[param_name] = kwargs[param_name]
            elif hasattr(self, param_name):
                new_k[param_name] = getattr(self, param_name)

        return cls(**new_k)

    def make_target(self, relative_file_path: str, use_unique_id: bool = True, processor: Optional[FileProcessor] = None) -> TargetOnKart:
        file_path = os.path.join(self.workspace_directory, relative_file_path)
        unique_id = self.make_unique_id() if use_unique_id else None
        return gokart.target.make_target(file_path=file_path, unique_id=unique_id, processor=processor)

    def make_large_data_frame_target(self, relative_file_path: str, use_unique_id: bool = True, max_byte=int(2**26)) -> TargetOnKart:
        file_path = os.path.join(self.workspace_directory, relative_file_path)
        unique_id = self.make_unique_id() if use_unique_id else None
        return gokart.target.make_model_target(
            file_path=file_path,
            temporary_directory=self.local_temporary_directory,
            unique_id=unique_id,
            save_function=gokart.target.LargeDataFrameProcessor(max_byte=max_byte).save,
            load_function=gokart.target.LargeDataFrameProcessor.load)

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
        return gokart.target.make_model_target(
            file_path=file_path,
            temporary_directory=self.local_temporary_directory,
            unique_id=unique_id,
            save_function=save_function,
            load_function=load_function)

    def load(self, target: Union[None, str, TargetOnKart] = None) -> Any:
        def _load(targets):
            if isinstance(targets, list) or isinstance(targets, tuple):
                return [_load(t) for t in targets]
            if isinstance(targets, dict):
                return {k: _load(t) for k, t in targets.items()}
            return targets.load()

        return _load(self._get_input_targets(target))

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

    def load_data_frame(self, target: Union[None, str, TargetOnKart] = None, required_columns: Optional[Set[str]] = None) -> pd.DataFrame:
        data = self.load(target=target)
        if isinstance(data, list):
            def _pd_concat(dfs):
                if isinstance(dfs, list):
                    return pd.concat([_pd_concat(df) for df in dfs])
                else:
                    return dfs
            data = _pd_concat(data)

        required_columns = required_columns or set()
        if data.empty:
            return pd.DataFrame(columns=required_columns)

        assert required_columns.issubset(set(data.columns)), f'data must have columns {required_columns}, but actually have only {data.columns}.'
        return data

    def dump(self, obj, target: Union[None, str, TargetOnKart] = None) -> None:
        self._get_output_target(target).dump(obj)

    def make_unique_id(self):
        self.task_unique_id = self.task_unique_id or self._make_hash_id()
        return self.task_unique_id

    def _make_hash_id(self):
        def _to_str_params(task):
            if isinstance(task, TaskOnKart):
                return str(task.make_unique_id())
            return task.to_str_params(only_significant=True)

        dependencies = [_to_str_params(task) for task in luigi.task.flatten(self.requires())]
        dependencies.append(self.to_str_params(only_significant=True))
        dependencies.append(self.__class__.__name__)
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
        self.dump(self.task_log, self._get_task_log_target())

    def _get_task_params_target(self):
        return self.make_target(f'log/task_params/{type(self).__name__}.pkl')

    def get_task_params(self) -> Dict:
        target = self._get_task_log_target()
        if target.exists():
            return self.load(target)
        return dict()

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
