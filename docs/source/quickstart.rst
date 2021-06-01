.. _quickstart:

Quickstart
==========

``remake`` works as a command-line tool, and as a Python package.

.. code-block:: bash

    remake setup-examples
    cd remake_examples
    remake run ex1

Alternatively, any ``remake`` file can be run using Python (e.g. using IPython):


::

    run ex1.py
    ex1.finalize()
    ex1.run_all()

