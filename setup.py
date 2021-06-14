#!/usr/bin/env python
from pathlib import Path
import subprocess as sp

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

version = sp.run('python remake/version.py',
                 check=True, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, encoding='utf8').stdout


def read(fname):
    try:
        return (Path(__file__).parent / fname).read_text()
    except (IOError, OSError, FileNotFoundError):
        return ''


setup(
    name='remake',
    version=version,
    description='Smart remake tool',
    include_package_data=True,
    license='LICENSE',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Mark Muetzelfeldt',
    author_email='mark.muetzelfeldt@reading.ac.uk',
    maintainer='Mark Muetzelfeldt',
    maintainer_email='mark.muetzelfeldt@reading.ac.uk',
    url='https://github.com/markmuetz/remake',
    project_urls={
        'Documentation': 'https://markmuetz.github.io/remake',
        'Bug Tracker': 'https://github.com/markmuetz/remake/issues',
    },
    packages=[
        'remake',
        'remake.examples',
        'remake.executor',
        'remake.experimental',
        ],
    python_requires='>=3.6',
    install_requires=[
        'networkx',
        'tabulate',
        ],
    extras_require={
        'debug': ['ipdb'],
        'display': ['matplotlib'],
        'experimental': ['numpy'],
        'testing': ['coverage', 'flake8', 'matplotlib', 'nose', 'numpy'],
    },
    entry_points={
        'console_scripts': [
            'remake=remake.remake_cmd:remake_cmd'
        ]
    },
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.9',
        'Development Status :: 4 - Beta',
        ],
    )
