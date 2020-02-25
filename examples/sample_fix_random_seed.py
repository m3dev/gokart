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
    # //----------------------------------------------------------------
    # Please set fix_random_seed_methods parameter.
    # Change seed if you change sample_param.
    #
    # //--- The output is as follows every time (installed pytorch). ---
    # {'random': [60, 27, 7, 56, 91, 38, 72, 37, 69, 51],
    #   'numpy': [20, 17, 22, 65, 87, 4, 30, 76, 81, 31],
    #   'torch': [0.4886833131313324, 0.8920509815216064,
    #     0.6191630959510803, -0.13227537274360657, -0.6677181720733643]}
    #
    # //--------------------- Not install pytorch ----------------------
    # {'random': [60, 27, 7, 56, 91, 38, 72, 37, 69, 51],
    #  'numpy': [20, 17, 22, 65, 87, 4, 30, 76, 81, 31],
    #  'torch': []}
    # //----------------------------------------------------------------
    gokart.run([
        'sample_fix_random_seed.SampleTask', '--local-scheduler', '--rerun', '--sample-param=a',
        '--fix-random-seed-methods=["random.seed","numpy.random.seed","torch.random.manual_seed"]'
    ])
