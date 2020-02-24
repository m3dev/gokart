import random

import gokart
import luigi
import numpy as np


class SampleTask(gokart.TaskOnKart):
    task_namespace = 'sample_fix_random_seed'
    sample_param = luigi.Parameter()

    def run(self):
        x = [random.randint(0, 100) for _ in range(0, 10)]
        y = [np.random.randint(0, 100) for _ in range(0, 10)]
        self.dump({'random': x, 'numpy': y})


if __name__ == '__main__':
    # The output is as follows every time.
    # {'random': [57, 2, 38, 65, 8, 26, 54, 14, 16, 41], 'numpy': [96, 65, 35, 38, 79, 22, 51, 56, 25, 62]}
    # Change seed if you change sample_param.
    gokart.run(['sample_fix_random_seed.SampleTask', '--local-scheduler', '--rerun', '--sample-param=a'])
