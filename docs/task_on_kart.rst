TaskOnKart
==========
``TaskOnKart`` inherits ``luigi.Task``, and has functions to make it easy to define tasks.
Please see `luigi documentation <https://luigi.readthedocs.io/en/stable/index.html>`_ for details of ``luigi.Task``.

Outline
--------
How ``TaskOnKart`` helps to define a task looks like:

.. code:: python

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
        luigi.build([TaskB(param='Hello')], local_scheduler=True)


TaskOnKart.make_target
----------------------
The :func:`~gokart.task.TaskOnKart.make_target` method is used to make an instance of ``Luigi.Target``.
For instance, an example implementation could be as follows:

.. code:: python

    def output(self):
        return self.make_target('output_file_name.pkl')

The ``make_target`` method adds `_{self.make_unique_id()}` to the file name as suffix.
In this case, the target file path is '{TaskOnKart.workspace_directory}/output_file_name_{self.make_unique_id()}.pkl'.


TaskOnKart.load
----------------
The :func:`~gokart.task.TaskOnKart.load` method is used to load input data.
For instance, an example implementation could be as follows:

.. code:: python

    def requires(self):
        return TaskA(param='called by TaskB')

    def run(self):
        # `load` loads input data. In this case, this loads the output of `TaskA`.
        output_of_task_a = self.load()


In the case that a task requires 2 or more tasks as input, the return value of this method has the same structure with `requires` value.
For instance, an example implementation that `requires` returns a dictionary of tasks could be like follows:

.. code:: python

    def requires(self):
        return dict(a=TaskA(), b=TaskB())

    def run(self):
        data = self.load() # returns dict(a=self.load('a'), b=self.load('b'))


The `load` method loads individual task input by passing a key of an input dictionary as follows:

.. code:: python

    def run(self):
        data_a = self.load('a')
        data_b = self.load('b')


TaskOnKart.dump
----------------
The :func:`~gokart.task.TaskOnKart.dump` method is used to dump results of tasks.
For instance, an example implementation could be as follows:

.. code:: python

    def output(self):
        return self.make_target('output.pkl')

    def run(self):
        results = do_something(self.load())
        self.dump(results)


In the case that a task has 2 or more output, it is possible to specify output target by passing a key of dictionary like follows:

.. code:: python

    def output(self):
        return dict(a=self.make_target('output_a.pkl'), b=self.make_target('output_b.pkl'))

    def run(self):
        a_data = do_something_a(self.load())
        b_data = do_something_b(self.load())
        self.dump(a_data, 'a')
        self.dump(b_data, 'b')

Advanced Features
---------------------

TaskOnKart.load_generator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :func:`~gokart.task.TaskOnKart.load_generator` method is used to load input data with generator.
For instance, an example implementation could be as follows:

.. code:: python

    def requires(self):
        return TaskA(param='called by TaskB')

    def run(self):
        for data in self.load_generator():
            any_process(data)


Usage is the same as `TaskOnKart.generator`.
`load_generator` reads the divided file into iterations.
It's effective when can't read all data to memory, because `load_generator` doesn't load all files at once.


TaskOnKart.fail_on_empty_dump
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Raise `AssertionError` on trying to dump empty dataframe.

Empty caches sometimes hide bugs and let us spend much time debugging. This feature notice us some bugs (including wrong datasources) in the early stage.
