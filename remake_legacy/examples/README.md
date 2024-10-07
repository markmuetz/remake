Examples
========

Can be run directly in the remake git repository, or in an examples directory set up using:

    remake setup-examples

Directory containing some examples of how to use `remake`. All examples can be run with:

    remake run

or individual examples with e.g.:

    remake run --reason ex1.py

All output can be reset by running:

    make clean

Suggested things to try
-----------------------

* Try editing any of the functions in `ex?.py`, then rerunning e.g. `remake run ex1.py`.
* Try editing the input files, then rerunning any example that depends on that file.
  - any tasks which depend on the output file will be run
  - if a task's output is not changed, further dependent tasks will not be run
