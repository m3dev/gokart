from setuptools import setup, find_packages

readme_note = """\
.. note::

   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/m3dev/gokart>`_\n\n
"""

with open('README.md') as f:
    long_description = readme_note + f.read()

install_requires = [
    'luigi',
    'python-dateutil==2.7.5',
    'boto3==1.7.84',
    'botocore== 1.10.84',
    'slackclient',
    'pandas',
    'numpy',
    'tqdm',
]

setup(
    name='gokart',
    version='0.1.12',
    description='A wrapper of luigi. This make it easy to define tasks.',
    long_description=long_description,
    author='M3, inc.',
    url='https://github.com/m3dev/gokart',
    license='MIT License',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=['moto==1.3.6'],
    test_suite='test',
    classifiers=['Programming Language :: Python :: 3.6.7'],
)
