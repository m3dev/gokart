import os
import luigi
import luigi.contrib.s3


class S3Config(luigi.Config):
    aws_access_key_id_name = luigi.Parameter(
        default='AWS_ACCESS_KEY_ID', description='AWS access key id environment variable.')
    aws_secret_access_key_name = luigi.Parameter(
        default='AWS_SECRET_ACCESS_KEY', description='AWS secret access key environment variable.')

    def get_s3_client(self) -> luigi.contrib.s3.S3Client:
        return luigi.contrib.s3.S3Client(
            aws_access_key_id=os.environ.get(self.aws_access_key_id_name),
            aws_secret_access_key=os.environ.get(self.aws_secret_access_key_name))
