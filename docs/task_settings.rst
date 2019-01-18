Task Settings
=============

Directory to Save Outputs
------------------------------------

We can use both a local directory and the S3 to save outputs.
To use local directory, please set a local directory path to :attr:`~gokart.task.TaskOnKart.workspace_directory`.
To use the S3 repository, please set ``s3://{YOUR_REPOSITORY_NAME}`` to :attr:`~gokart.task.TaskOnKart.workspace_directory`,
and appropriate values to the parameters of :class:`~gokart.s3_config.S3Config`.