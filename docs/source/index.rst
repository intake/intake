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

.. note::

    We are making Take2 as a full release. It is still "beta" in the sense that we will be adding
    many data types, readers and transformers, and are prepared to revisit the API in general. The
    reason not to use a pre-release or RC, is that users never see these.


.. warning::

    Looking for :ref:`v1` documentation? You may have just installed Intake and found that
    Take2 broke things for you, so you might wish to pin to an older version. Or stick around
    and find out why you might wish to update your code. All old "sources", whether still working
    or not, should be considered deprecated.


.. toctree::
    :maxdepth: 2

    scope2.rst
    user2.rst
    walkthrough2.rst
    tour2.rst
    api2.rst

Install
-------

To install Intake Take2:

.. code-block:: bash

    pip install -c conda-forge intake
    or
    pip install intake

Please leave issues and discussions on our `repo page`_.

.. _repo page: https://github.com/intake/intake

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. raw:: html

    <script data-goatcounter="https://intake.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
