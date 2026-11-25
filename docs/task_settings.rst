Task Settings
=============

Task settings. Also please refer to :doc:`task_parameters` section.


Directory to Save Outputs
-------------------------

We can use both a local directory and the S3 to save outputs.
If you would like to use local directory, please set a local directory path to :attr:`~gokart.task.TaskOnKart.workspace_directory`. Please refer to :doc:`task_parameters` for how to set it up.

It is recommended to use the config file since it does not change much.

::

    # base.ini
    [TaskOnKart]
    workspace_directory=${TASK_WORKSPACE_DIRECTORY}

.. code:: python

    # main.py
    import gokart
    gokart.add_config('base.ini')


To use the S3 or GCS repository, please install the corresponding extras and set the bucket path as ``s3://{YOUR_REPOSITORY_NAME}`` or ``gs://{YOUR_REPOSITORY_NAME}`` respectively.

.. code:: sh

    pip install gokart[s3]   # for S3 support
    pip install gokart[gcs]  # for GCS support
    pip install gokart[all]  # for both S3 and GCS

Also, please set credential information to Environment Variables.

.. code:: sh

    # S3
    export AWS_ACCESS_KEY_ID='~~~'  # AWS access key
    export AWS_SECRET_ACCESS_KEY='~~~'  # AWS secret access key

    # GCS
    export GCS_CREDENTIAL='~~~'  # GCS credential: a service account key file path or its JSON content

If ``GCS_CREDENTIAL`` is not set, `Application Default Credentials <https://cloud.google.com/docs/authentication/application-default-credentials>`_ are used.

gokart accesses GCS through `google-cloud-storage <https://cloud.google.com/python/docs/reference/storage/latest>`_.
To access GCS from your own code with the same credentials, use ``GCSConfig().get_storage_client()``, which returns a ``google.cloud.storage.Client``.

.. code:: python

    from gokart.gcs_config import GCSConfig

    client = GCSConfig().get_storage_client()
    blob = client.bucket('YOUR_BUCKET').get_blob('path/to/object.pkl')

Large files can be uploaded and downloaded with several connections at once (google-cloud-storage's `transfer_manager <https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.transfer_manager>`_).
This is off by default. Set a size threshold to turn it on.
Sizes are bytes (``268435456``) or a number with a unit: ``KB`` / ``MB`` / ``GB`` are powers of 1000 and ``KiB`` / ``MiB`` / ``GiB`` are powers of 1024 (``M`` alone is rejected as ambiguous).

.. code:: ini

    [GCSConfig]
    ; upload files / download objects of 256 MiB or more in parallel
    parallel_upload_threshold=256MiB
    parallel_download_threshold=256MiB
    ; size of each chunk (default 32MiB; 5MiB to 5GiB for uploads and at least 1MiB for downloads) and concurrent requests (default 8)
    parallel_chunk_size=32MiB
    parallel_max_workers=8

- Invalid settings raise a ``ValueError`` when a GCS target is created, not when the first large file is transferred.
- A file or object of one chunk or less is always transferred in one stream.
- Parallel uploads use an `XML API multipart upload <https://cloud.google.com/storage/docs/multipart-uploads>`_.
  The uploaded object has a CRC32C checksum but no MD5 hash, and the caller needs the ``storage.multipartUploads.*`` permissions (the predefined Storage Object Creator / User / Admin roles have them).
  Each part is read into memory before it is sent, so an upload holds up to ``parallel_chunk_size`` × ``parallel_max_workers`` bytes (chunks grow above ``parallel_chunk_size`` for a file of more than 10,000 chunks).
  If the process is killed during an upload, the incomplete upload stays in the bucket and is billed; an ``AbortIncompleteMultipartUpload`` lifecycle rule on the bucket removes it.
- Labels are sent as HTTP headers in a parallel upload, so a file whose labels cannot be HTTP headers (e.g. non-ASCII text, a ``:`` in a label name, or an upper case letter in a label name, which the XML API would lowercase) is uploaded in one stream instead.
- Objects stored with ``Content-Encoding: gzip`` are always downloaded in one stream, since GCS does not serve byte ranges of them while decompressing.
- The storage client keeps up to 10 connections per host, so a ``parallel_max_workers`` above 10 reopens connections and logs "Connection pool is full" warnings.
- With ``parallel_download_threshold`` set, every download first reads the object's metadata to get its size, which adds one request per download.
  The download then reads the generation found by that request, so an object overwritten in the meantime is never read as a mix of the old and new content: the download raises ``NotFound`` (or, with Object Versioning, returns the old content).

.. warning::
   ``GCSConfig().get_gcs_client()``, which returns a ``luigi.contrib.gcs.GCSClient`` built on ``google-api-python-client``, is deprecated and will be removed in gokart 2.0.0 (`#685 <https://github.com/m3dev/gokart/issues/685>`_), together with ``google-api-python-client`` in the ``gcs`` extra.
   Use ``GCSConfig().get_storage_client()`` instead.
   Errors from GCS I/O in gokart are now ``google.api_core.exceptions.GoogleAPICallError`` subclasses (e.g. ``NotFound``) instead of ``googleapiclient.errors.HttpError``.


Rerun task
----------

There are times when we want to rerun a task, such as when change script or on batch. Please use the ``rerun`` parameter or add an arbitrary parameter.


When set rerun as follows:

.. code:: python

    # rerun TaskA
    gokart.build(Task(rerun=True))


When used from an argument as follows:

.. code:: python

    # main.py
    class Task(gokart.TaskOnKart[str]):
        def run(self):
            self.dump('hello')

.. code:: sh

    python main.py Task --local-scheduler --rerun


``rerun`` parameter will look at the dependent tasks up to one level.

Example: Suppose we have a straight line pipeline composed of TaskA, TaskB and TaskC,  and TaskC is an endpoint of this pipeline. We also suppose that all the tasks have already been executed.

- TaskA(rerun=True)  ->  TaskB  ->  TaskC    # not rerunning
- TaskA  ->  TaskB(rerun=True)  ->  TaskC    # rerunning TaskB and TaskC

This is due to the way intermediate files are handled. ``rerun`` parameter is ``significant=False``, it does not affect the hash value. It is very important to understand this difference.


If you want to change the parameter of TaskA and rerun TaskB and TaskC, recommend adding an arbitrary parameter.

.. code:: python

    class TaskA(gokart.TaskOnKart):
        __version: luigi.IntParameter = luigi.IntParameter(default=1)

If the hash value of TaskA will change, the dependent tasks (in this case, TaskB and TaskC) will rerun.


Check modification times
------------------------

Set ``modification_time_check=True`` to rerun a task when an input is newer than an output.
When naive and timezone-aware timestamps are mixed, comparisons convert them to UTC instants.
Naive timestamps are interpreted in the system's local timezone, allowing local, in-memory, S3 and GCS targets to be mixed.
Custom targets whose timestamps represent another timezone should return timezone-aware ``datetime`` objects.
Mixed comparisons require timestamps that the Python runtime can convert with ``datetime.astimezone()``; unconvertible calendar-boundary values are unsupported.


Fix random seed
---------------

Every task has a parameter named :attr:`~gokart.task.TaskOnKart.fix_random_seed_methods` and :attr:`~gokart.task.TaskOnKart.fix_random_seed_value`. This can be used to fix the random seed.


.. code:: python

    import gokart
    import random
    import numpy
    import torch

    class Task(gokart.TaskOnKart[dict[str, Any]]):
        def run(self):
            x = [random.randint(0, 100) for _ in range(0, 10)]
            y = [np.random.randint(0, 100) for _ in range(0, 10)]
            z = [torch.randn(1).tolist()[0] for _ in range(0, 5)]
            self.dump({'random': x, 'numpy': y, 'torch': z})

    gokart.build(
        Task(
            fix_random_seed_methods=[
                "random.seed",
                "numpy.random.seed",
                "torch.random.manual_seed"],
            fix_random_seed_value=57))

::

    # //--- The output is as follows every time. ---
    # {'random': [65, 41, 61, 37, 55, 81, 48, 2, 94, 21],
    #   'numpy': [79, 86, 5, 22, 79, 98, 56, 40, 81, 37], 'torch': []}
    #   'torch': [0.14460121095180511, -0.11649507284164429,
    #            0.6928958296775818, -0.916053831577301, 0.7317505478858948]}

This will be useful for using Machine Learning Libraries.
