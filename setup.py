#!/usr/bin/env python
import os

try:
    from setuptools import setup, Extension
except ImportError:
    from distutils.core import setup, Extension


def read(fname):
    try:
        return open(os.path.join(os.path.dirname(__file__), fname)).read()
    except:
        return ''


setup(
    name='remake',
    version='0.3',
    description='',
    long_description=read('README.md'),
    author='Mark Muetzelfeldt',
    author_email='mark.muetzelfeldt@reading.ac.uk',
    maintainer='Mark Muetzelfeldt',
    maintainer_email='mark.muetzelfeldt@reading.ac.uk',
    packages=[
        'remake',
        ],
    scripts=[
        ],
    python_requires='>=3.6',
    install_requires=[
        ],
    classifiers=[
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Natural Language :: English',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 3.6',
        ],
    keywords=[''],
    )
