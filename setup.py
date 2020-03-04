#!/usr/bin/env python
from pathlib import Path

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension

from remake.version import get_version


def read(fname):
    try:
        return (Path(__file__) / fname).read_text()
    except (IOError, OSError, FileNotFoundError):
        return ''


setup(
    name='remake',
    version=get_version(),
    description='Smart remake tool',
    long_description=read('README.md'),
    author='Mark Muetzelfeldt',
    author_email='mark.muetzelfeldt@reading.ac.uk',
    maintainer='Mark Muetzelfeldt',
    maintainer_email='mark.muetzelfeldt@reading.ac.uk',
    packages=[
        'remake',
        ],
    python_requires='>=3.6',
    install_requires=[
        ],
    extras_require={
        'debug': ['ipdb'],
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
        'Programming Language :: Python :: 3.6',
        ],
    keywords=[''],
    )
