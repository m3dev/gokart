import warnings
from logging import getLogger

import luigi

from gokart.task import TaskOnKart

logger = getLogger(__name__)


def make_tree_info(task, indent='', last=True, details=False, abbr=True, visited_tasks=None):
    """
    Return a string representation of the tasks, their statuses/parameters in a dependency tree format
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(action='ignore', message='Task .* without outputs has no custom complete() method')
        is_task_complete = task.complete()
    is_complete = ('COMPLETE' if is_task_complete else 'PENDING')
    result = '\n' + indent
    if last:
        result += '└─-'
        indent += '   '
    else:
        result += '|--'
        indent += '|  '
    name = task.__class__.__name__
    result += f'({is_complete}) {name}[{task.make_unique_id()}]'

    if abbr:
        visited_tasks = visited_tasks or set()
        task_id = f'{name}_{task.make_unique_id()}'
        if task_id not in visited_tasks:
            visited_tasks.add(task_id)
        else:
            result += f'\n{indent}└─- ...'
            return result

    if details:
        params = task.get_info(only_significant=True)
        output_paths = [t.path() for t in luigi.task.flatten(task.output())]
        processing_time = task.get_processing_time()
        if type(processing_time) == float:
            processing_time = str(processing_time) + 's'
        result += f'(parameter={params}, output={output_paths}, time={processing_time}, task_log={dict(task.get_task_log())})'

    children = luigi.task.flatten(task.requires())
    for index, child in enumerate(children):
        result += make_tree_info(child, indent, (index + 1) == len(children), details=details, abbr=abbr, visited_tasks=visited_tasks)
    return result


class tree_info(TaskOnKart):
    mode = luigi.Parameter(default='', description='This must be in ["simple", "all"].')  # type: str
    output_path = luigi.Parameter(default='tree.txt', description='Output file path.')  # type: str

    def output(self):
        return self.make_target(self.output_path, use_unique_id=False)
