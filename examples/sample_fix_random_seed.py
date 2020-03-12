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
        try:
            import torch
            z = [torch.randn(1).tolist()[0] for _ in range(0, 5)]
        except ImportError:
            z = []
        self.dump({'random': x, 'numpy': y, 'torch': z})


if __name__ == '__main__':
    # //---------------------------------------------------------------------
    # Please set fix_random_seed_methods parameter.
    # Change seed if you change sample_param.
    #
    # //--- The output is as follows every time (with pytorch installed). ---
    # {'random': [65, 41, 61, 37, 55, 81, 48, 2, 94, 21],
    #   'numpy': [79, 86, 5, 22, 79, 98, 56, 40, 81, 37], 'torch': []}
    #   'torch': [0.14460121095180511, -0.11649507284164429,
    #            0.6928958296775818, -0.916053831577301, 0.7317505478858948]}
    #
    # //------------------------- without pytorch ---------------------------
    # {'random': [65, 41, 61, 37, 55, 81, 48, 2, 94, 21],
    #   'numpy': [79, 86, 5, 22, 79, 98, 56, 40, 81, 37], 'torch': []}
    #
    # //---------------------------------------------------------------------
    gokart.run([
        'sample_fix_random_seed.SampleTask', '--local-scheduler', '--rerun', '--sample-param=a',
        '--fix-random-seed-methods=["random.seed","numpy.random.seed","torch.random.manual_seed"]', '--fix-random-seed-value=57'
    ])
