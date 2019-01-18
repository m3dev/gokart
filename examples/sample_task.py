import luigi

import gokart


class TaskA(gokart.TaskOnKart):
    param = luigi.Parameter()

    def output(self):
        return self.make_target('output_of_task_a.pkl')

    def run(self):
        results = f'param={self.param}'
        self.dump(results)


class TaskB(gokart.TaskOnKart):
    param = luigi.Parameter()

    def requires(self):
        return TaskA(param='called by TaskB')

    def output(self):
        # `make_target` makes an instance of `luigi.Target`.
        # This infers the output format and the destination of an output objects.
        # The target file path is
        #     '{TaskOnKart.workspace_directory}/output_of_task_b_{self.make_unique_id()}.pkl'.
        return self.make_target('output_of_task_b.pkl')

    def run(self):
        # `load` loads input data. In this case, this loads the output of `TaskA`.
        output_of_task_a = self.load()
        results = f'"{output_of_task_a}" is loaded in TaskB.'
        # `dump` writes `results` to the file path of `self.output()`.
        self.dump(results)


if __name__ == '__main__':
    # luigi.build([TaskB(param='Hello')], local_scheduler=True)
    # gokart.run(['--tree-info-mode=simple', '--tree-info-output-path=tree_simple.txt', 'TaskB', '--param=Hello', '--local-scheduler'])
    gokart.run(['--tree-info-mode=all', '--tree-info-output-path=tree_all.txt', 'TaskB', '--param=Hello', '--local-scheduler'])
