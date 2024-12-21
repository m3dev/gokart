# gokart

<p align="center">
  <img src="https://raw.githubusercontent.com/m3dev/gokart/master/docs/gokart_logo_side_isolation.svg" width="90%">
<p>

[![Test](https://github.com/m3dev/gokart/workflows/Test/badge.svg)](https://github.com/m3dev/gokart/actions?query=workflow%3ATest)
[![](https://readthedocs.org/projects/gokart/badge/?version=latest)](https://gokart.readthedocs.io/en/latest/)
[![Python Versions](https://img.shields.io/pypi/pyversions/gokart.svg)](https://pypi.org/project/gokart/)
[![](https://img.shields.io/pypi/v/gokart)](https://pypi.org/project/gokart/)
![](https://img.shields.io/pypi/l/gokart)

Gokart solves reproducibility, task dependencies, constraints of good code, and ease of use for Machine Learning Pipeline.


[Documentation](https://gokart.readthedocs.io/en/latest/) for the latest release is hosted on readthedocs.


# About gokart

Here are some good things about gokart.

- The following meta data for each Task is stored separately in a `pkl` file with hash value
    - task output data
    - imported all module versions
    - task processing time
    - random seed in task
    - displayed log
    - all parameters set as class variables in the task
- Automatically rerun the pipeline if parameters of Tasks are changed.
- Support GCS and S3 as a data store for intermediate results of Tasks in the pipeline.
- The above output is exchanged between tasks as an intermediate file, which is memory-friendly
- `pandas.DataFrame` type and column checking during I/O
- Directory structure of saved files is automatically determined from structure of script
- Seeds for numpy and random are automatically fixed
- Can code while adhering to [SOLID](https://en.wikipedia.org/wiki/SOLID) principles as much as possible
- Tasks are locked via redis even if they run in parallel

**All the functions above are created for constructing Machine Learning batches. Provides an excellent environment for reproducibility and team development.**


Here are some non-goal / downside of the gokart.
- Batch execution in parallel is supported, but parallel and concurrent execution of task in memory.
- Gokart is focused on reproducibility. So, I/O and capacity of data storage can become a bottleneck.
- No support for task visualize.
- Gokart is not an experiment management tool. The management of the execution result is cut out as [Thunderbolt](https://github.com/m3dev/thunderbolt).
- Gokart does not recommend writing pipelines in toml, yaml, json, and more. Gokart is preferring to write them in Python.

# Getting Started

Within the activated Python environment, use the following command to install gokart.

```
pip install gokart
```


# Quickstart

## Minimal Example

A minimal gokart tasks looks something like this:


```python
import gokart

class Example(gokart.TaskOnKart):
    def run(self):
        self.dump('Hello, world!')

task = Example()
output = gokart.build(task)
print(output)
```

`gokart.build` return the result of dump by `gokart.TaskOnKart`. The example will output the following.


```
Hello, world!
```

## Type-Safe Pipeline Example

We introduce type-annotations to make a gokart pipeline robust.
Please check the following example to see how to use type-annotations on gokart.
Before using this feature, ensure to enable [mypy plugin](https://gokart.readthedocs.io/en/latest/mypy_plugin.html) feature in your project.

```python
import gokart

# `gokart.TaskOnKart[str]` means that the task dumps `str`
class StrDumpTask(gokart.TaskOnKart[str]):
    def run(self):
        self.dump('Hello, world!')

# `gokart.TaskOnKart[int]` means that the task dumps `int`
class OneDumpTask(gokart.TaskOnKart[int]):
    def run(self):
        self.dump(1)

# `gokart.TaskOnKart[int]` means that the task dumps `int`
class TwoDumpTask(gokart.TaskOnKart[int]):
    def run(self):
        self.dump(2)

class AddTask(gokart.TaskOnKart[int]):
    # `a` requires a task to dump `int`
    a: gokart.TaskOnKart[int] = gokart.TaskInstanceParameter()
    # `b` requires a task to dump `int`
    b: gokart.TaskOnKart[int] = gokart.TaskInstanceParameter()

    def requires(self):
        return dict(a=self.a, b=self.b)

    def run(self):
        # loading by instance parameter,
        # `a` and `b` are treated as `int`
        # because they are declared as `gokart.TaskOnKart[int]`
        a = self.load(self.a)
        b = self.load(self.b)
        self.dump(a + b)


valid_task = AddTask(a=OneDumpTask(), b=TwoDumpTask())
# the next line will show type error by mypy
# because `StrDumpTask` dumps `str` and `AddTask` requires `int`
invalid_task = AddTask(a=OneDumpTask(), b=StrDumpTask())
```

This is an introduction to some of the gokart.
There are still more useful features.

Please See [Documentation](https://gokart.readthedocs.io/en/latest/) .

Have a good gokart life.

# Achievements

Gokart is a proven product.

- It's actually been used by [m3.inc](https://corporate.m3.com/en) for over 3 years
- Natural Language Processing Competition by [Nishika.inc](https://nishika.com) 2nd prize : [Solution Repository](https://github.com/vaaaaanquish/nishika_akutagawa_2nd_prize)


# Thanks

gokart is a wrapper for luigi. Thanks to luigi and dependent projects!

- [luigi](https://github.com/spotify/luigi)
