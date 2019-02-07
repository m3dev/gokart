Task Information
================

Task Tree
---------

There are two ways to print the significant parameters and state of the task and its dependencies in a tree format.
One is to use luigi module. See `luigi.tools.deps_tree module <https://luigi.readthedocs.io/en/stable/api/luigi.tools.deps_tree.html>`_ for details.
Another is to use ``task-info`` option which is implemented in gokart.

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


Notify Job Status to Slack
---------------------------
When :class:`~gokart.slack.slack_config.SlackConfig` is set, ``gokart.run`` notices task results at the end of pipeline.
