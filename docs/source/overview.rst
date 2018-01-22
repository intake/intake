Overview
========

Intake is a Python API for bulk loading of data.  It consists of three parts:

1. A lightweight plugin system for adding data loaders for new file formats and servers (like databases and REST endpoints)
2. A YAML-based catalog file for describing data sources that you can load by name
3. A catalog server and client that can share data catalog metadata over the network, or even stream the data directly to clients if needed

Intake supports loading data into standard Python containers, depending on the data source:

* Pandas Dataframes - tabular data
* NumPy Arrays - tensor data
* Python lists of dictionaries - semi-structured data
* (possibly more in the future)

Additionally, Intake can load data into distributed data structures.  Currently it supports Dask, a flexible parallel computing library with distributed containers like `dask.dataframe <https://dask.pydata.org/en/latest/dataframe.html>`_, `dask.array <https://dask.pydata.org/en/latest/array.html>`_, and `dask.bag <https://dask.pydata.org/en/latest/bag.html>`_.  In the future, other distributed computing systems could use Intake to create similar data structures.

Why Intake?
-----------

Intake tries to solve a related set of problems:

* Python API standards for loading data (such as DB-API 2.0) are optimized for transactional databases and query results that are processed one row at a time.
* Libraries that do load data in bulk tend to each have their own API for doing so, which adds friction when switching data formats.
* Loading data into a distributed data structure (like those found in Dask and Spark) often require writing a separate loader.
* Abstractions often focus on just one data model (tabular, n-dimensional array, or semi-structured), when many projects need to work with multiple kinds of data.

This effort has some similarity to Blaze, but with the explicit goal of **not** defining a computational expression system.  Intake plugins load the data into containers that provide their data processing features.  As a result, it is very easy to make a new Intake plugin with a relatively small amount of Python.

Concepts
--------

Intake is built out of four core concepts:

* Plugin: An object with an `open()` method that can create Data Source objects given some plugin-specific arguments.
* Data Source: An object that represents a reference to a data source.  Data source objects have methods for loading the data into standard containers, like Pandas DataFrames and Dask containers, but do not load any data until specifically requested.
* Catalog: A collection of catalog entries.  Catalog objects can be created from local YAML defitions, or by connecting to remote servers.
* Catalog Entry: A named data source.  The catalog entry includes metadata about the source, as well as the name of the plugin and arguments for the `open()` method to Catalog entries can be parameterized, allowing one entry to return different subsets of data depending on the user request.

Plugins can be used directly by the user, or indirectly through data catalogs.  Data sources can be pickled, sent over the network to other hosts, and reopened (assuming the remote system has access to the required files or servers).


Future Directions
-----------------

We are interested in several future directions for Intake:

* Tighter integration with Arrow for network transport and more easily creating plugins around data loader libraries that create Arrow data structures.
* Support for loading data into PySpark data structures (similar to Dask)
* A Blaze backend that can read Intake data sources and uses Dask for computation.
* Odo integration so that Intake data sources can be converted to other formats.
