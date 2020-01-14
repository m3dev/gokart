from setuptools import setup, find_packages
from codecs import open
from os import path

with open(path.join(path.abspath(path.dirname(__file__)), 'README.md'), encoding='utf-8') as f:
    long_description = f.read()


install_requires = [
    'luigi',
    'boto3',
    'slackclient>=2.0.0',
    'pandas',
    'numpy',
    'tqdm',
    'google-auth',
    'pyarrow',
    'uritemplate',
    'google-api-python-client'
]

setup(
    name='gokart',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description='A wrapper of luigi. This make it easy to define tasks.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    author='M3, inc.',
    url='https://github.com/m3dev/gokart',
    license='MIT License',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=['moto==1.3.6'],
    test_suite='test',
    classifiers=['Programming Language :: Python :: 3.6'],
)
