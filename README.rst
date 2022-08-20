========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |github-actions| |requires|
        | |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/lazy_qa/badge/?style=flat
    :target: https://lazy_qa.readthedocs.io/
    :alt: Documentation Status

.. |github-actions| image:: https://github.com/angeleyesffx/lazy_qa/actions/workflows/github-actions.yml/badge.svg
    :alt: GitHub Actions Build Status
    :target: https://github.com/angeleyesffx/lazy_qa/actions

.. |requires| image:: https://requires.io/github/angeleyesffx/lazy_qa/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/angeleyesffx/lazy_qa/requirements/?branch=master

.. |codecov| image:: https://codecov.io/gh/angeleyesffx/lazy_qa/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/angeleyesffx/lazy_qa

.. |version| image:: https://img.shields.io/pypi/v/lazy-qa.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/lazy-qa

.. |wheel| image:: https://img.shields.io/pypi/wheel/lazy-qa.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/lazy-qa

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/lazy-qa.svg
    :alt: Supported versions
    :target: https://pypi.org/project/lazy-qa

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/lazy-qa.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/lazy-qa

.. |commits-since| image:: https://img.shields.io/github/commits-since/angeleyesffx/lazy_qa/v0.0.0.svg
    :alt: Commits since latest release
    :target: https://github.com/angeleyesffx/lazy_qa/compare/v0.0.0...master



.. end-badges



* Free software: MIT license

Installation
============

::

    pip install lazy-qa

You can also install the in-development version with::

    pip install https://github.com/angeleyesffx/lazy_qa/archive/master.zip


Documentation
=============


https://lazy_qa.readthedocs.io/


Development
===========

To run all the tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
