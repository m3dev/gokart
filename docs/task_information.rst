Task Information
================

There are 6 ways to print the significant parameters and state of the task and its dependencies.

* 1. One is to use luigi module. See `luigi.tools.deps_tree module <https://luigi.readthedocs.io/en/stable/api/luigi.tools.deps_tree.html>`_ for details.
* 2. ``task-info`` option of ``gokart.run()``.
* 3. ``make_task_info_as_tree_str()`` will return significant parameters and dependency tree as str.
* 4. ``make_task_info_as_table()`` will return significant parameter and dependent tasks as pandas.DataFrame table format.
* 5. ``dump_task_info_table()`` will dump the result of ``make_task_info_as_table()`` to a file.
* 6. ``dump_task_info_tree()`` will dump the task tree object (TaskInfo) to a pickle file.


This document will cover 2~6.

2. task-info option of gokart.run()
--------------------------------------------

On CLI
~~~~~~

An example implementation could be like:

.. code:: python

    # main.py

    import gokart

    if __name__ == '__main__':
        gokart.run()


.. code:: sh

    $ python main.py \
        TaskB \
        --param=Hello \
        --local-scheduler \
        --tree-info-mode=all \
        --tree-info-output-path=tree_all.txt


The ``--tree-info-mode`` option accepts "simple" and "all", and a task information is saved in ``--tree-info-output-path``.

when "simple" is passed, it outputs the states and the unique ids of tasks.
An example output is as follows:

.. code:: text

    └─-(COMPLETE) TaskB[09fe5591ef2969ce7443c419a3b19e5d]
       └─-(COMPLETE) TaskA[2549878535c070fb6c3cd4061bdbbcff]



When "all" is passed, it outputs the states, the unique ids, the significant parameters, the execution times and the task logs of tasks.
An example output is as follows:

.. code:: text

    └─-(COMPLETE) TaskB[09fe5591ef2969ce7443c419a3b19e5d](parameter={'workspace_directory': './resources/', 'local_temporary_directory': './resources/tmp/', 'param': 'Hello'}, output=['./resources/output_of_task_b_09fe5591ef2969ce7443c419a3b19e5d.pkl'], time=0.002290010452270508s, task_log={})
       └─-(COMPLETE) TaskA[2549878535c070fb6c3cd4061bdbbcff](parameter={'workspace_directory': './resources/', 'local_temporary_directory': './resources/tmp/', 'param': 'called by TaskB'}, output=['./resources/output_of_task_a_2549878535c070fb6c3cd4061bdbbcff.pkl'], time=0.0009829998016357422s, task_log={})



3. make_task_info_as_tree_str()
-----------------------------------------

``gokart.tree.task_info.make_task_info_as_tree_str()`` will return a tree dependency tree as a str.

.. code:: python

    from gokart.tree.task_info import make_task_info_as_tree_str

    make_task_info_as_tree_str(task, ignore_task_names)
    # Parameters
    # ----------
    # - task: TaskOnKart
    #     Root task.
    # - details: bool
    #     Whether or not to output details.
    # - abbr: bool
    #     Whether or not to simplify tasks information that has already appeared.
    # - ignore_task_names: Optional[List[str]]
    #     List of task names to ignore.
    # Returns
    # -------
    # - tree_info : str
    #     Formatted task dependency tree.


example

.. code:: python

    import luigi
    import gokart

    class TaskA(gokart.TaskOnKart[str]):
        param = luigi.Parameter()
        def run(self):
            self.dump(f'{self.param}')

    class TaskB(gokart.TaskOnKart[str]):
        task: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()
        def run(self):
            task = self.load('task')
            self.dump(task + ' taskB')

    class TaskC(gokart.TaskOnKart[str]):
        task: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()
        def run(self):
            task = self.load('task')
            self.dump(task + ' taskC')

    class TaskD(gokart.TaskOnKart):
        task1: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()
        task2: gokart.TaskOnKart[str] = gokart.TaskInstanceParameter()
        def run(self):
            task = [self.load('task1'), self.load('task2')]
            self.dump(','.join(task))


.. code:: python

    task = TaskD(
        task1=TaskD(
            task1=TaskD(task1=TaskC(task=TaskA(param='foo')), task2=TaskC(task=TaskB(task=TaskA(param='bar')))),  # same task
            task2=TaskD(task1=TaskC(task=TaskA(param='foo')), task2=TaskC(task=TaskB(task=TaskA(param='bar'))))   # same task
        ),
        task2=TaskD(
            task1=TaskD(task1=TaskC(task=TaskA(param='foo')), task2=TaskC(task=TaskB(task=TaskA(param='bar')))),  # same task
            task2=TaskD(task1=TaskC(task=TaskA(param='foo')), task2=TaskC(task=TaskB(task=TaskA(param='bar'))))   # same task
        )
    )
    print(gokart.make_task_info_as_tree_str(task))


.. code:: sh

    └─-(PENDING) TaskD[187ff82158671283e127e2e1f7c9c095]
        |--(PENDING) TaskD[ca9e943ce049e992b371898c0578784e]    # duplicated TaskD
        |  |--(PENDING) TaskD[1cc9f9fc54a56614f3adef74398684f4]    # duplicated TaskD
        |  |  |--(PENDING) TaskC[dce3d8e7acaf1bb9731fb4f2ae94e473]
        |  |  |  └─-(PENDING) TaskA[be65508b556dd3752359b4246791413d]
        |  |  └─-(PENDING) TaskC[de39593d31490aba3cdca3c650432504]
        |  |     └─-(PENDING) TaskB[bc2f7d6cdd6521cc116c35f0f144eed3]
        |  |        └─-(PENDING) TaskA[5a824f7d232eb69d46f0ac6bbd93b565]
        |  └─-(PENDING) TaskD[1cc9f9fc54a56614f3adef74398684f4]
        |     └─- ...
        └─-(PENDING) TaskD[ca9e943ce049e992b371898c0578784e]
            └─- ...


In the above example, the sub-trees already shown is omitted.
This can be disabled by passing ``False`` to ``abbr`` flag:

.. code:: python

    print(make_task_info_as_tree_str(task, abbr=False))


4. make_task_info_as_table()
--------------------------------

``gokart.tree.task_info.make_task_info_as_table()`` will return a table containing the information of significant parameters and dependent tasks as a pandas DataFrame.
This table contains `task name`, `cache unique id`, `cache file path`, `task parameters`, `task processing time`, `completed flag`, and `task log`.

.. code:: python

    from gokart.tree.task_info import make_task_info_as_table

    make_task_info_as_table(task, ignore_task_names)
    # """Return a table containing information about dependent tasks.
    #
    # Parameters
    # ----------
    # - task: TaskOnKart
    #     Root task.
    # - ignore_task_names: Optional[List[str]]
    #     List of task names to ignore.
    # Returns
    # -------
    # - task_info_table : pandas.DataFrame
    #     Formatted task dependency table.
    # """


5. dump_task_info_table()
-----------------------------------------

``gokart.tree.task_info.dump_task_info_table()`` will dump the task_info table made at ``make_task_info_as_table()`` to a file.

.. code:: python

    from gokart.tree.task_info import dump_task_info_table

    dump_task_info_table(task, task_info_dump_path, ignore_task_names)
    # Parameters
    # ----------
    # - task: TaskOnKart
    #     Root task.
    # - task_info_dump_path: str
    #     Output target file path. Path destination can be `local`, `S3`, or `GCS`.
    #     File extension can be any type that gokart file processor accepts, including `csv`, `pickle`, or `txt`.
    #     See `TaskOnKart.make_target module <https://gokart.readthedocs.io/en/latest/task_on_kart.html#taskonkart-make-target>` for details.
    # - ignore_task_names: Optional[List[str]]
    #     List of task names to ignore.
    # Returns
    # -------
    # None


6. dump_task_info_tree()
-----------------------------------------

``gokart.tree.task_info.dump_task_info_tree()`` will dump the task tree object (TaskInfo) to a pickle file.

.. code:: python

    from gokart.tree.task_info import dump_task_info_tree

    dump_task_info_tree(task, task_info_dump_path, ignore_task_names, use_unique_id)
    # Parameters
    # ----------
    # - task: TaskOnKart
    #     Root task.
    # - task_info_dump_path: str
    #     Output target file path. Path destination can be `local`, `S3`, or `GCS`.
    #     File extension must be '.pkl'.
    # - ignore_task_names: Optional[List[str]]
    #     List of task names to ignore.
    # - use_unique_id: bool = True
    #     Whether to use unique id to dump target file. Default is True.
    # Returns
    # -------
    # None


Task Logs
---------
To output extra information of tasks by ``tree-info``, the member variable :attr:`~gokart.task.TaskOnKart.task_log` of ``TaskOnKart`` keeps any information as a dictionary.

For instance, the following code runs,

.. code:: python

    import gokart


    class SampleTaskLog(gokart.TaskOnKart):
        def run(self):
            # Add some logs.
            self.task_log['sample key'] = 'sample value'


    if __name__ == '__main__':
        SampleTaskLog().run()
        gokart.run([
            '--tree-info-mode=all',
            '--tree-info-output-path=sample_task_log.txt',
            'SampleTaskLog',
            '--local-scheduler'])


the output could be like:

.. code:: text

    └─-(COMPLETE) SampleTaskLog[...](..., task_log={'sample key': 'sample value'})


Delete Unnecessary Output Files
--------------------------------
To delete output files which are not necessary to run a task, add option ``--delete-unnecessary-output-files``. This option is supported only when a task outputs files in local storage not S3 for now.
