TaskOnKart
==========
``TaskOnKart`` inherits ``luigi.Task``, and has functions to make it easy to define tasks.
Please see `luigi documentation <https://luigi.readthedocs.io/en/stable/index.html>`_ for details of ``luigi.Task``.

Please refer to :doc:`intro_to_gokart` section and :doc:`tutorial` section.


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
            return TaskA(param='world')

        def output(self):
            # `make_target` makes an instance of `luigi.Target`.
            # This infers the output format and the destination of an output objects.
            # The target file path is
            #     '{self.workspace_directory}/output_of_task_b_{self.make_unique_id()}.pkl'.
            return self.make_target('output_of_task_b.pkl')

        def run(self):
            # `load` loads input data. In this case, this loads the output of `TaskA`.
            output_of_task_a = self.load()
            results = f'Task A: {output_of_task_a}\nTaskB: param={self.param}'
            # `dump` writes `results` to the file path of `self.output()`.
            self.dump(results)


    if __name__ == '__main__':
        print(gokart.build([TaskB(param='Hello')]))


The result of this script will look like this

.. code:: sh

    Task A: param=world
    Task B: param=Hello

The results are obtained as a pipeline by linking A and B.


TaskOnKart.make_target
----------------------
The :func:`~gokart.task.TaskOnKart.make_target` method is used to make an instance of ``Luigi.Target``.
For instance, an example implementation could be as follows:

.. code:: python

    def output(self):
        return self.make_target('file_name.pkl')

The ``make_target`` method adds ``_{self.make_unique_id()}`` to the file name as suffix.
In this case, the target file path is ``{self.workspace_directory}/file_name_{self.make_unique_id()}.pkl``.


It is also possible to specify a file format other than pkl. The supported file formats are as follows:

- .pkl
- .txt
- .csv
- .tsv
- .gz
- .json
- .xml

If dump something other than the above, can use :func:`~gokart.TaskOnKart.make_model_target`.
Please refer to :func:`~gokart.task.TaskOnKart.make_target` and described later Advanced Features section.


.. note::
    By default, file path is inferred from "__name__" of the script, so ``output`` method can be omitted.
    Please refer to :doc:`tutorial` section.


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


We can also omit the :func:`~gokart.task.TaskOnKart.requires` and write the task used by :func:`~gokart.parameter.TaskInstanceParameter`.
Extensions include :func:`~gokart.task.TaskOnKart.load_data_frame` and :func:`~gokart.task.TaskOnKart.load_generator`. Please refer to :func:`~gokart.task.TaskOnKart.load`, :doc:`setting_task_parameters`, and described later Advanced Features section.


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

Please refer to :func:`~gokart.task.TaskOnKart.dump`.


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

Please refer to :func:`~gokart.task.TaskOnKart.load_generator`.


TaskOnKart.load_data_frame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :func:`~gokart.task.TaskOnKart.load_data_frame` method is used to load input ``pandas.DataFrame``.

.. code:: python

    def requires(self):
        return MakeDataFrameTask()

    def run(self):
        df = self.load_data_frame(required_columns={'colA', 'colB'}, drop_columns=True)

This allows us to omit ``reset_index`` and ``drop`` when loading. And if there is a missing column, ``AssertionError`` will be raised. Useful for pipelines based on pandas.

Please refer to :func:`~gokart.task.TaskOnKart.load_data_frame`.


TaskOnKart.fail_on_empty_dump
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :func:`~gokart.task.TaskOnKart.fail_on_empty_dump` method is `AssertionError` on trying to dump empty ``pandas.DataFrame``.

.. code:: python

    def run(self):
        df = pd.DataFrame()
        self.fail_on_empty_dump(df)  # AssertionError

Empty caches sometimes hide bugs and let us spend much time debugging. This feature notice us some bugs (including wrong datasources) in the early stage.

Please refer to :func:`~gokart.task.TaskOnKart.fail_on_empty_dump`.


TaskOnKart.make_model_target
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :func:`~gokart.task.TaskOnKart.make_model_target` method is used to dump for non supported file types.

.. code:: python
    import gensim

    class TrainWord2Vec(gokart.TaskOnKart):
        def output(self):
            # please use 'zip'.
            return self.make_model_target(
                'model.zip',
                save_function=gensim.model.Word2Vec.save,
                load_function=gensim.model.Word2Vec.load)

        def run(self):
            # -- train word2vec ---
            word2vec = train_word2vec()
            self.dump(word2vec)

It is dumped and zipped with ``gensim.model.Word2Vec.save``.

Please refer to :func:`~gokart.task.TaskOnKart.make_model_target`.
