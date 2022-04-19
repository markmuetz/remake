.. _release:

Release
=======

.. code-block:: bash

    # Update remake/version.py
    cd docs/
    make
    cd ..
    python setup.py sdist
    git commit -a
    git tag v<release>
    twine upload dist/remake-<release>.tar.gz
    git push && git push --tags
