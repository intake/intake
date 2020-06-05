Overview
========

Introduction
------------

This page describes the technical design of Intake, with brief details of the aims of the project and
components of the library

Why Intake?
-----------

Intake solves a related set of problems:

* Python API standards for loading data (such as DB-API 2.0) are optimized for transactional databases and query results
  that are processed one row at a time.

* Libraries that do load data in bulk tend to each have their own API for doing so, which adds friction when switching
  data formats.

* Loading data into a distributed data structure (like those found in Dask and Spark) often requires writing a separate
  loader.

* Abstractions often focus on just one data model (tabular, n-dimensional array, or semi-structured), when many projects
  need to work with multiple kinds of data.

Intake has the explicit goal of **not** defining a computational expression
system.  Intake plugins load the data into containers (e.g., arrays or data-frames) that
provide their data processing features.  As a result, it is
very easy to make a new Intake plugin with a relatively small amount of Python.

Structure
---------

Intake is a Python library for accessing data in a simple and uniform way.  It consists of three parts:

1. A lightweight plugin system for adding data loader :term:`drivers<Driver>` for new file formats and servers
(like databases, REST endpoints or other cataloging services)

2. A cataloging system for specifying these sources in simple :term:`YAML` syntax, or with plugins that read source specs
from some external data service

3. A server-client architecture that can share data catalog metadata over the network, or even stream the data directly
to clients if needed

Intake supports loading data into standard Python containers. The list can be easily extended,
but the currently supported list is:

* Pandas Dataframes - tabular data

* NumPy Arrays - tensor data

* Python lists of dictionaries - semi-structured data

Additionally, Intake can load data into distributed data structures.  Currently it supports Dask, a flexible parallel
computing library with distributed containers like `dask.dataframe <https://dask.pydata.org/en/latest/dataframe.html>`_,
`dask.array <https://dask.pydata.org/en/latest/array.html>`_,
and `dask.bag <https://dask.pydata.org/en/latest/bag.html>`_.
In the future, other distributed computing systems could use Intake to create similar data structures.

Concepts
--------

Intake is built out of four core concepts:

* Data Source classes: the "driver" plugins that each implement loading of some specific type of data into python, with
  plugin-specific arguments.

* Data Source: An object that represents a reference to a data source.  Data source instances have methods for loading the
  data into standard containers, like Pandas DataFrames, but do not load any data until specifically requested.

* Catalog: A collection of catalog entries, each of which defines a Data Source. Catalog objects can be created from
  local YAML definitions, by connecting
  to remote servers, or by some driver that knows how to query an external data service.

* Catalog Entry: A named data source held internally by catalog objects, which generate
  data source instances when accessed.
  The catalog entry includes metadata about the source, as well as the name of the
  driver and arguments. Arguments can be parameterized, allowing one entry to return
  different subsets of data depending on the user request.

The business of a plugin is to go from some data format (bunch of files or some remote service)
to a ":term:`Container`" of the data (e.g., data-frame), a thing on which you can perform further analysis.
Drivers can be used directly by the user, or indirectly through data catalogs.  Data sources can be pickled, sent over
the network to other hosts, and reopened (assuming the remote system has access to the required files or servers).

See also the :doc:`glossary`.

Future Directions
-----------------

Ongoing work for enhancements, as well as requests for plugins, etc., can be found at the
`issue tracker <https://github.com/intake/intake/issues>`_. See the :ref:`roadmap`
for general mid- and long-term goals.
