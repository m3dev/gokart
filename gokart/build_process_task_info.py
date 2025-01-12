import io
import logging

import gokart
import gokart.tree.task_info
from gokart.build import TaskDumpConfig, TaskDumpMode, TaskDumpOutputType
from gokart.task import TaskOnKart

logger: logging.Logger = logging.getLogger(__name__)


def process_task_info(task: TaskOnKart, task_dump_config: TaskDumpConfig = TaskDumpConfig()):
    match task_dump_config:
        case TaskDumpConfig(mode=TaskDumpMode.NONE, output_type=TaskDumpOutputType.NONE):
            pass
        case TaskDumpConfig(mode=TaskDumpMode.TREE, output_type=TaskDumpOutputType.PRINT):
            tree = gokart.make_tree_info(task)
            logger.info(tree)
        case TaskDumpConfig(mode=TaskDumpMode.TABLE, output_type=TaskDumpOutputType.PRINT):
            table = gokart.tree.task_info.make_task_info_as_table(task)
            output = io.StringIO()
            table.to_csv(output, index=False, sep='\t')
            output.seek(0)
            logger.info(output.read())
        case TaskDumpConfig(mode=TaskDumpMode.TREE, output_type=TaskDumpOutputType.DUMP):
            tree = gokart.make_tree_info(task)
            gokart.TaskOnKart().make_target(f'log/task_info/{type(task).__name__}.txt').dump(tree)
        case TaskDumpConfig(mode=TaskDumpMode.TABLE, output_type=TaskDumpOutputType.DUMP):
            table = gokart.tree.task_info.make_task_info_as_table(task)
            gokart.TaskOnKart().make_target(f'log/task_info/{type(task).__name__}.pkl').dump(table)
        case _:
            raise ValueError(f'Unsupported TaskDumpConfig: {task_dump_config}')
