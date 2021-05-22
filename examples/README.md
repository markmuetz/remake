Examples
========

Directory containing some examples of how to use `remake`. All examples can be run with:

    remake run

or individual examples with e.g.:

    remake run --reason ex1.py

All output can be reset by running:

    make clean

Suggested things to try
-----------------------

* Try editing the output files, then rerunning `make run_examples`.
  - any tasks which depend on the output file will be run
  - if a task's output is not changed, further dependent tasks will not be run
* Try editing any of the functions in `ex?.py`, then rerunning `make run_examples`.
