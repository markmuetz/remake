.. remake documentation master file, created by
   sphinx-quickstart on Tue Jun  1 19:48:34 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

remake |version| documentation
==============================

Introduction
------------

Remake is a smart Python build tool, similar to ``make``. It is file based -- all inputs and outputs of each task are files. It uses a pure-Python implementation to define a set of tasks, where any tasks can depend on the output from previous tasks. It makes it easy to define complex task graphs, using a filename formatter for each task to define its inputs and outputs. It is smart, in that if any of the tasks or any of the input files to a task's content changes, those tasks will be rerun. Subsequent tasks will only be rerun if their input has changed.

Remake tracks the contents of each file and task, and can be used to generate a report of how any particular file was made. It is particularly suited to use in scientific settings, due to its ability to reliably recreate any set of output files, based on running only those tasks that are necessary.

.. toctree::
    :maxdepth: 2
    :caption: Contents:

    installation
    quickstart
    tutorial

.. toctree::   
    :maxdepth: 2
    :caption: Reference:

    remake_cli
    remake_package

Indices and tables
------------------

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
