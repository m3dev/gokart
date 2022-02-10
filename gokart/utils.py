import os

import luigi


def add_config(file_path: str):
    _, ext = os.path.splitext(file_path)
    luigi.configuration.core.parser = ext
    assert luigi.configuration.add_config_path(file_path)
