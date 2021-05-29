#!/usr/bin/env python
from pathlib import Path

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from remake.version import get_version


def read(fname):
    try:
        return (Path(__file__).parent / fname).read_text()
    except (IOError, OSError, FileNotFoundError):
        return ''

setup(
    name='remake',
    version=get_version(),
    description='Smart remake tool',
    license = 'LICENSE',
    long_description=read('README.md'),
    long_description_content_type='text/markdown',
    author='Mark Muetzelfeldt',
    author_email='mark.muetzelfeldt@reading.ac.uk',
    maintainer='Mark Muetzelfeldt',
    maintainer_email='mark.muetzelfeldt@reading.ac.uk',
    url='https://github.com/markmuetz/remake',
    project_urls={
        'Bug Tracker': 'https://github.com/markmuetz/remake/issues',
    },
    packages=[
        'remake',
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
        'testing': ['nose', 'coverage'],
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
        ],
    )
