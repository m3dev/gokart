import hashlib
import os
from typing import Union, List, Any, Callable, Set, Optional

import luigi
import pandas as pd
import gokart
from gokart.target import TargetOnKart


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
        default='./resources/',
        description='A directory to set outputs on. Please use a path starts with s3:// when you use s3.')  # type: str
    local_temporary_directory = luigi.Parameter(
        default='./resources/tmp/', description='A directory to save temporary files.')  # type: str
    rerun = luigi.BoolParameter(
        default=False, description='If this is true, this task will run even if all output files exist.')
    strict_check = luigi.BoolParameter(
        default=False, description='If this is true, this task will not run only if all input and output files exits.')

    def complete(self) -> bool:
        if self.rerun:
            self.rerun = False
            return False

        targets = luigi.task.flatten(self.output())

        if self.strict_check:
            targets += luigi.task.flatten(self.input())
        return all([t.exists() for t in targets])

    def make_target(self, relative_file_path: str, use_unique_id: bool = True) -> TargetOnKart:
        file_path = os.path.join(self.workspace_directory, relative_file_path)
        unique_id = self._make_unique_id() if use_unique_id else None
        return gokart.target.make_target(file_path=file_path, unique_id=unique_id)

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
        unique_id = self._make_unique_id() if use_unique_id else None
        return gokart.target.make_model_target(
            file_path=file_path,
            temporary_directory=self.local_temporary_directory,
            unique_id=unique_id,
            save_function=save_function,
            load_function=load_function)

    def load(self, target: Union[None, str, TargetOnKart] = None) -> Any:
        def _load(targets):
            if isinstance(targets, list):
                return [_load(t) for t in targets]
            if isinstance(targets, dict):
                return {k: _load(t) for k, t in targets.items()}
            return targets.load()

        return _load(self._get_input_targets(target))

    def load_data_frame(self,
                        target: Union[None, str, TargetOnKart] = None,
                        required_columns: Optional[Set[str]] = None) -> pd.DataFrame:
        data = self.load(target=target)
        if isinstance(data, list):
            data = pd.concat(data)

        required_columns = required_columns or set()
        if data.empty:
            return pd.DataFrame(columns=required_columns)

        assert required_columns.issubset(set(
            data.columns)), f'data must have columns {required_columns}, but actually have only {data.columns}.'
        return data

    def dump(self, obj, target: Union[None, str, TargetOnKart] = None) -> None:
        self._get_output_target(target).dump(obj)

    def _make_unique_id(self):
        def _to_str_params(task):
            if isinstance(task, TaskOnKart):
                return str(task._make_unique_id())
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
