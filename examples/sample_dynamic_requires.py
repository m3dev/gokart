import luigi

import gokart
'''
Executes a Task using the results of a Task as dynamic parameters.

For example, when we create a Task that generates model parameters.
TaskA: model parameter tuning task. output: optimized parameter dictionary.
TaskB: training model task.
TaskC: output trained model file using optimized parameter from TaskA.

We can using `gokart.TaskInstanceParameter` in gokart.
Code: https://github.com/m3dev/gokart/blob/master/gokart/parameter.py

luigi's dynamic-dependencies: https://luigi.readthedocs.io/en/stable/tasks.html#dynamic-dependencies
'''


class TaskA(gokart.TaskOnKart):
    '''Generate parameters dynamically.'''
    sample = luigi.Parameter()

    def run(self):
        results = {'param_a': 1, 'param_b': self.sample}
        self.dump(results)


class TaskB(gokart.TaskOnKart):
    '''Training model.'''
    task = gokart.TaskInstanceParameter()

    def requires(self):
        return self.task

    def run(self):
        params = self.load()
        params.update({'trained': True})  # training model
        self.dump(params)


class TaskC(gokart.TaskOnKart):
    '''Output trained model file using optimized parameter from TaskA.'''
    def requires(self):
        return TaskB(task=self.clone(TaskA, sample='hoge'))

    def run(self):
        model = self.load()
        model.update({'task_name': 'task_c'})
        self.dump(model)


if __name__ == '__main__':
    '''
    ./resource/
    └─ __main__/
       ├── TaskA_74416d6e12945172d2ae8a4eaa6bc9de.pkl   # {'param_a': 1, 'param_b': 'hoge'}
       ├── TaskB_c48d5b5c44a9d87b0e3be5b7dbc2df68.pkl    # {'param_a': 1, 'param_b': 'hoge', 'trained': True}
       └── TaskC_12de1c6b5eca6cc86d18424d5d1e16e5.pkl   # {'param_a': 1, 'param_b': 'hoge', 'trained': True, 'task_name': 'task_c'}
    '''
    gokart.run(['TaskC', '--local-scheduler'])