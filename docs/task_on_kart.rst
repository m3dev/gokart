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


    class TaskA(gokart.TaskOnKart[str]):
        param: luigi.Parameter = luigi.Parameter()

        def output(self):
            return self.make_target('output_of_task_a.pkl')

        def run(self):
            results = f'param={self.param}'
            self.dump(results)


    class TaskB(gokart.TaskOnKart[str]):
        param: luigi.Parameter = luigi.Parameter()

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
- .ndjson
- .xml
- .npz
- .parquet
- .feather
- .png
- .jpg
- .ini


If dump something other than the above, can use :func:`~gokart.TaskOnKart.make_model_target`.
Please refer to :func:`~gokart.task.TaskOnKart.make_target` and described later Advanced Features section.


.. note::
    By default, file path is inferred from "__name__" of the script, so ``output`` method can be omitted.
    Please refer to :doc:`tutorial` section.

.. note::
    When using `.feather`, index will be converted to column at saving and restored to index at loading.
    If you don't prefere saving index, set `store_index_in_feather=False` parameter at `gokart.target.make_target()`.

.. note::
    When you set `serialized_task_definition_check=True`, the task will rerun when you modify the scripts of the task.
    Please note that the scripts outside the class are not considered.



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


As an alternative, the `load` method loads individual task input by passing an instance of TaskOnKart as follows:

.. code:: python

    def run(self):
        data_a = self.load(TaskA())
        data_b = self.load(TaskB())


We can also omit the :func:`~gokart.task.TaskOnKart.requires` and write the task used by :func:`~gokart.parameter.TaskInstanceParameter`.
Also please refer to :func:`~gokart.task.TaskOnKart.load`, :doc:`task_parameters`, and described later Advanced Features section.


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


TaskOnKart.make_model_target
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The :func:`~gokart.task.TaskOnKart.make_model_target` method is used to dump for non supported file types.

.. code:: python

    import gensim

    class TrainWord2Vec(gokart.TaskOnKart[Word2VecResult]):
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


TaskOnKart.fail_on_empty_dump
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Please refer to :doc:`for_pandas`.


TaskOnKart.should_dump_supplementary_log_files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Whether to dump supplementary files (task_log, random_seed, task_params, processing_time, module_versions) or not. Default is True.

Note that when set to False, task_info functions (e.g. gokart.tree.task_info.make_task_info_as_tree_str()) cannot be used.


Dump csv with encoding
~~~~~~~~~~~~~~~~~~~~~~~

You can dump csv file by implementing `Task.output()` method as follows:

.. code:: python

    def output(self):
        return self.make_target('file_name.csv')

By default, csv file is dumped with `utf-8` encoding.

If you want to dump csv file with other encodings, you can use `encoding` parameter as follows:

.. code:: python

    from gokart.file_processor import CsvFileProcessor

    def output(self):
        return self.make_target('file_name.csv', processor=CsvFileProcessor(encoding='cp932'))
        # This will dump csv as 'cp932' which is used in Windows.

Keep output in memory instead of dumping to files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can keep the output of a task in memory instead of dumping it to a file by passing ``persist=False`` to :func:`~gokart.task.TaskOnKart.make_target`.
This avoids creating cache files and the I/O to the file system or to object storage such as GCS and S3.
It is useful for intermediate outputs that you would otherwise recompute with ``rerun=True`` every time.

Please note that this is an experimental feature.

.. code:: python

    class Feature(gokart.TaskOnKart[pd.DataFrame]):
        def output(self):
            return self.make_target('feature.pkl', persist=False)

        def run(self):
            self.dump(make_feature())

The key of the output is made from the file path and the unique id, in the same way as a file target.

- **Lifetime**: the outputs are kept only during one ``gokart.build`` call. Every ``gokart.build`` starts with no in-memory outputs, so the tasks that have them always run, and the outputs are dropped when ``gokart.build`` returns.
- **Early release**: an output is removed from memory as soon as every task that requires it has finished, except the output of the task passed to ``gokart.build``, which is returned. Outputs are kept until the build ends instead when a task in the build uses ``strict_check`` or ``modification_time_check`` (``complete()`` of such a task checks the outputs of the dependencies of its dependencies), or yields dynamic dependencies from ``run()`` (a yielded output might have been removed already). The latter is detected only when ``run()`` itself is a generator function; if a removed output is yielded in another way, the task is run again after the luigi scheduler's ``[scheduler] stable_done_cooldown_secs`` (10 seconds by default).
- **Shallow copy**: for a ``pandas.DataFrame``, ``dump()`` stores and ``load()`` returns a shallow copy (``copy(deep=False)``), so changes made after ``dump()`` by the dumping task, or by a task that loads the output, do not leak to other tasks, and Copy-on-Write keeps the values from being changed. The copy shares the data buffers with the original and adds only the column and index metadata; with Copy-on-Write, only a block that is modified later is copied. Any other object is stored and returned as is without copying: if a task modifies it, other tasks that load it see the change.
- **Only in gokart.build and in a single process**: an error is raised when the output is used outside ``gokart.build`` (e.g. ``luigi.build`` or ``gokart.run``), or with ``workers > 1``, ``[gokart_worker] force_multiprocessing``, or a worker timeout, because these run tasks in separate processes.
- **Threads**: threads started in ``run()`` can load and dump in-memory outputs, also while ``run()`` calls ``gokart.build``: the outer and the nested builds keep their outputs apart, unless both output the same key, in which case such a thread dumps it to the nested build and, once the nested build has it, loads it from there too. Running ``gokart.build`` calls that have in-memory outputs in multiple threads at the same time is not supported, and the later one raises ``InMemoryTargetError``. Builds without in-memory outputs are not affected and can run in other threads at the same time.
- **No lock**: ``should_lock_run=True`` raises an error. When only a lock backend is configured (``url`` of ``[gokart_lock]``, or the deprecated ``redis_host`` and ``redis_port``), the output is not locked at dump and load.
- ``processor`` cannot be used with ``persist=False``. ``make_model_target`` and ``make_large_data_frame_target`` do not support it.
- **Redis client still needed**: even though the output is not locked, a task needs ``gokart[redis]`` when it is created if a Redis lock backend is configured.
- **No log files**: when all outputs of a task use ``persist=False``, the supplementary log files (``log/task_log/``, ``log/task_params/``, ``log/random_seed/``, ``log/processing_time/`` and ``log/module_versions/``) are not dumped either. The random seed is still fixed with ``fix_random_seed_methods``. Because of this, task information tools such as ``gokart.tree.task_info`` cannot show these files for such tasks.
