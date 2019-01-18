import gokart
from gokart.info import tree_info


class SampleTaskLog(gokart.TaskOnKart):
    def run(self):
        self.task_log['sample key'] = 'sample value'


if __name__ == '__main__':
    SampleTaskLog().run()
    tree_info()
    gokart.run(['--tree-info-mode=all', '--tree-info-output-path=sample_task_log.txt', 'SampleTaskLog', '--local-scheduler'])
