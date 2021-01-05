# gokart

[![Build Status](https://travis-ci.org/m3dev/gokart.svg)](https://travis-ci.org/m3dev/gokart)
[![](https://readthedocs.org/projects/gokart/badge/?version=latest)](https://gokart.readthedocs.io/en/latest/)
[![](https://img.shields.io/pypi/v/gokart)](https://pypi.org/project/gokart/)
![](https://img.shields.io/pypi/l/gokart)

A wrapper of the data pipeline library "luigi".


## Getting Started
Run `pip install gokart` to install the latest version from PyPI. [Documentation](https://gokart.readthedocs.io/en/latest/) for the latest release is hosted on readthedocs.

## How to Use
Please use gokart.TaskOnKart instead of luigi.Task to define your tasks.


### Basic Task with gokart.TaskOnKart
```python
import gokart

class BasicTask(gokart.TaskOnKart):
    def requires(self):
        return TaskA()

    def output(self):
        # please use TaskOnKart.make_target to make Target.
        return self.make_target('basic_task.csv')

    def run(self):
        # load data which TaskA output
        texts = self.load()
        
        # do something with texts, and make results.
        
        # save results with the file path {self.workspace_directory}/basic_task_{unique_id}.csv
        self.dump(results)
``` 

### Details of base functions
#### Make Target with TaskOnKart
`TaskOnKart.make_target` judge `Target` type by the passed path extension. The following extensions are supported.
 
 - pkl
 - txt
 - csv
 - tsv
 - gz
 - json
 - xml

#### Make Target for models which generate multiple files in saving.
`TaskOnKart.make_model_target` and `TaskOnKart.dump` are designed to save and load models like gensim.model.Word2vec. 
```python
class TrainWord2Vec(TaskOnKart):
    def output(self):
        # please use 'zip'.
        return self.make_model_target(
            'model.zip', 
            save_function=gensim.model.Word2Vec.save,
            load_function=gensim.model.Word2Vec.load)

    def run(self):
        # make word2vec
        self.dump(word2vec)
```

#### Load input data
##### Pattern 1: Load input data individually.
```python
def requires(self):
    return dict(data=LoadItemData(), model=LoadModel())

def run(self):
    # pass a key in the dictionary `self.requires()`
    data = self.load('data')  
    model = self.load('model')
```

##### Pattern 2: Load input data at once
```python
def run(self):
    input_data = self.load()
    """
    The above line is equivalent to the following:
    input_data = dict(data=self.load('data'), model=self.load('model'))
    """
```


#### Load input data as pd.DataFrame
```python
def requires(self):
    return LoadDataFrame()

def run(self):
    data = self.load_data_frame(required_columns={'id', 'name'})  
```

## Advanced
### Task cache collision lock
#### Require
You need to install [redis](https://redis.io/topics/quickstart) for this advanced function.

#### Description
Task lock is implemented to prevent task cache collision.
(Originally, task cache collision may occur when same task with same parameters run at different applications parallelly.) 

1. Set up a redis server at somewhere accessible from gokart/luigi jobs.

    Following will run redis at your localhost.

    ```bash
    $ redis-server
    ```

2. Set redis server hostname and port number as parameters to gokart.TaskOnKart().

    You can set it by adding `--redis-host=[your-redis-localhost] --redis-port=[redis-port-number]` options to gokart python script.

    e.g.
    ```bash
    
    python main.py sample.SomeTask --local-scheduler --redis-host=localhost --redis-port=6379
    ```

    Alternatively, you may set parameters at config file.

    ```conf.ini
    [TaskOnKart]
    redis_host=localhost
    redis_port=6379
    ```

### Inherit task parameters with decorator
#### Description
```python
class MasterConfig(luigi.Config):
    param: str = luigi.Parameter()
    param2: str = luigi.Parameter()

@inherits_config_params(MasterConfig)
class SomeTask(gokart.TaskOnKart):
    param: str = luigi.Parameter()
```

This is useful when multiple tasks has same parameter, since parameter settings of `MasterConfig`  will be inherited to all tasks decorated with `@inherits_config_params(MasterConfig)`.

Note that parameters which exists in both `MasterConfig` and `SomeTask` will be inherited.
In the above example, `param2` will not be available in `SomeTask`, since `SomeTask` does not have `param2` parameter.