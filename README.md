remake [![Build Status](https://github.com/markmuetz/remake/actions/workflows/python-package.yml/badge.svg)](https://github.com/markmuetz/remake/actions/workflows/python-package.yml) [![codecov](https://codecov.io/gh/markmuetz/remake/branch/main/graph/badge.svg)](https://codecov.io/gh/markmuetz/remake) 
======

Remake is a smart Python build tool, similar to `make`. It is file based - all inputs and outputs of each task are files. It uses a pure-Python implementation to define a set of tasks, where any task can depend on the output from previous tasks. It makes it easy to define complex task graphs by defining a subclass of `Rule`, using a filename formatter for each task to define its inputs and outputs. It is smart, in that if any of the tasks , those tasks will be rerun. Subsequent tasks will only be rerun as needed.

Remake is file aware - it tracks each file and task - and can be used to generate a report of how any particular file was made. It is particularly suited to use for scientific workflows, due to its ability to reliably recreate any set of output files, based on running only those tasks that are necessary.
