import sys

import luigi
import numpy as np

import gokart
from gokart import getLogger

logger = getLogger(__name__)


class Sample(gokart.TaskOnKart):
    task_namespace = 'log_test'

    def output(self):
        logger.critical('critical')
        logger.exception('exception')
        logger.warning('waring')
        logger.fatal('fatal')
        return self.make_target('data/sample.pkl')

    def run(self):
        self.dump('sample output')


if __name__ == '__main__':
    luigi.configuration.LuigiConfigParser.add_config_path('./param.ini')
    np.random.seed(57)
    gokart.run(sys.argv[1:])
