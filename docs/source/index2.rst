.. raw:: html

   <img src="_static/images/logo.png" alt="Intake Logo" style="float:right;width:94px;height:60px;">

.. _take2:

Intake Take2
============

*Taking the pain out of data access and distribution*

Intake is an open-source package to:

- describe your data declaratively
- gather data sets into catalogs
- search catalogs and services to find the right data you need
- load, transform and output data in many formats
- work with third party remote storage and compute platforms

This is the start of the documentation for the alpha version of Intake: Take2, a
rewrite of Intake (henceforth referred to as legacy or V1). We will give an
introduction to the ideas of Intake in general and specifically how to use this
release. Go directly to the walkthrough and examples, or read the following motivation
and declarations of scope.

.. toctree::
    :maxdepth: 2

    scope2.rst
    walkthrough2.rst
    tour2.rst

Install
-------

To install Intake Take2:

.. code-block:: bash

    pip install --pre intake
    or
    pip install intake==2.0.0a1

This being an _alpha_ release, we would like as much feedback from the community as possible!
Please leave issues and discussions on our `repo page`_.

.. _repo page: https://github.com/intake/intake
