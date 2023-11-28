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
release.

.. toctree::
    :maxdepth: 2

    walkthrough2.rst

Install
-------

This page and its children describes Intake Take2, the successor to Intake. Whether
you are familiar with Intake V1 ("legacy") or not, we will motivate what this package is
for, what it can do for you and why you should use it.

To install Intake Take2:

.. code-block:: bash

    pip install --pre intake
    or
    pip install intake==2.0.0a1

This being an _alpha_ release, we would like as much feedback from the community as possible!
Please leave issues and discussions on our `repo page`_.

.. _repo page: https://github.com/intake/intake

Motivation
----------

Data scientists, analysis, ML/AI developers and engineers want to spend their time working
with data to produce models, results and insights. Those first few lines or cells in a workflow
to get to the data and annoying, often wrong and brittle.
Consider the following cell of realistic-looking code.

.. image:: ./_static/images/complex_cell.png
   :alt: Complex Cell

This cell encodes several steps it takes to produce the object of interest for the user, including
defining some location URL, fetching data, loading, cleaning and converting data. It refers to
multiple packages, and hard-codes various arguments directly. This is clearly messy code, but it
is extremely common - in fact, most data-centric workflows start like this. Now imagine finding
and using this dataset for a new project, or when the upstream location, format or types have changed.
Further more, the steps are highly specific - if you decide you need a different compute engine,
or the data moves to a different storage service, it is painful to fix this. If the same dataset is
used in many places, you will need to remember to fix it in each place.


Wouldn't it be nice if you could declare your data and pipeline just once? Then you can
- version control your data sets as you would for data
- share your data definitions with others, just by writing this prescription to any shared space
- update all users of this data in a single place
- encode a set of transforms as part of a data-oriented processing framework
- decide to load and process the same data but with different engines
- automatically encode python statements into data decriptions
- convert between dozens of data types



Relationship to V1
------------------

We aim to be largely backward compatible with pre-V2 Intake sources and catalogs.
Many data sources have been rewritten
to use the new framework, and many rarely-used features have been removed. In particular, the
following features are no longer supported for V1 sources:

- the intake server (use ``tiled`` instead)
- caching (use ``fsspec`` caching instead or custom caching pipelines)
- "persist" and "export" (use the new converters and output classes)
- automatic ``hvplot`` (this is now an "output" converter)
- some niche source features such as CSV file pattern matching

In addition, not all existing ``intake_*`` packages have corresponding readers in Take2, but we are
making progress.
