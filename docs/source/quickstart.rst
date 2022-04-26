.. _quickstart:

Quickstart
==========

``remake`` works as a command-line tool, and as a Python package.

.. code-block:: bash

    remake setup-examples
    cd remake-examples
    remake run ex1
    cat README.md

Alternatively, any ``remake`` file can be run using Python (e.g. using IPython):

::

    >>> from remake import load_remake
    >>> ex1 = load_remake('ex1.py', finalize=True)
    >>> ex1.run_all()

