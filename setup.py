from setuptools import setup, find_packages
from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip

readme_note = """\
.. note::

   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/m3dev/gokart>`_\n\n
"""

with open('README.md') as f:
    long_description = readme_note + f.read()

pfile = Project(chdir=False).parsed_pipfile
requirements = convert_deps_to_pip(pfile['packages'], r=False)

setup(
    name='gokart',
    version='0.1.10',
    description='A wrapper of luigi. This make it easy to define tasks.',
    long_description=long_description,
    author='M3, inc.',
    url='https://github.com/m3dev/gokart',
    license='MIT License',
    install_requires=requirements,
    packages=find_packages(),
    classifiers=[
        'Programming Language :: Python :: 3.7',
    ],
)
