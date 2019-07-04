from setuptools import setup, find_packages

readme_note = """\
.. note::

   For the latest source, discussion, etc, please visit the
   `GitHub repository <https://github.com/m3dev/gokart>`_\n\n
"""

install_requires = [
    'luigi',
    'python-dateutil==2.7.5',
    'boto3',
    'slackclient>=2.0.0',
    'pandas',
    'numpy',
    'tqdm',
]

setup(
    name='gokart',
    use_scm_version=True,
    setup_requires=['setuptools_scm'],
    description='A wrapper of luigi. This make it easy to define tasks.',
    author='M3, inc.',
    url='https://github.com/m3dev/gokart',
    license='MIT License',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=['moto==1.3.6'],
    test_suite='test',
    classifiers=['Programming Language :: Python :: 3.6'],
)
