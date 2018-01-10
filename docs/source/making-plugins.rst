Making Plugins
==============

The goal of the Intake plugin system is to make it very simple to implement a plugin for a new data source, without any special knowledge of Dask or the Intake catalog system.

Assumptions
-----------

Although Intake is very flexible about data, there are some basic assumptions that a plugin must satisfy.

Data Model
''''''''''

Intake currently supports 3 kinds of containers, represented the most common data models used in Python:

* dataframe
* ndarray
* python (list of dictionaries)

A given plugin must only return one kind of container.  If a file format (such as HDF5) could reasonably be interpreted as two different data models depending on usage (such as a dataframe or an ndarray), then two different plugins need to be created with different names.

The source of data should be essentially permanent and immutable.  That is, loading the data should not destroy or modify the data, nor should closing the data source destroy the data either.  When a data source is serialized and sent to another host, it will need to be reopened at the destination, which may cause queries to be re-executed and files to be reopened.  Data sources that treat readers as "consumers" and remove data once read will cause erratic behavior, so Intake is not suitable for accessing things like message queues.

Schema
''''''

The schema of a data source is assumed to be knowable prior to loading the data in full.  However, different kinds of data sources may not be able to determine their schema entirely ahead of time.  For example, determining the length of a CSV data set requires scanning the entire file, and many databases only report the field names and types after executing a query.  In these cases, there may be unknown parts of the schema before the read is executed.  Plugins may choose to have the user input this unknown information in the `open()` method, or do some kind of partial data inspection to determine the schema.  Regardless of method used, schema determination should be roughly independent of the number of returned records.

Partitioning
''''''''''''

Data sources are assumed to be *partitionable*.  A data partion is a randomly accessible fragment of the data.  Partitions are numbered, starting from zero, and correspond to contiguous chunks of data divided along the first index of the data structure.

.. note::

    Representation of ndarrays partitioned along multiple axes is TBD, but a desired feature.

Not all data sources can be partitioned.  For example, arbitrary SQL queries generally cannot be partitioned, and file formats without sufficient indexing often can only be read from beginning to end.  In these cases, the DataSource object should report that there is only 1 partition.  However, it often makes sense for a data source to be able to represent a directory of files, in which case the files can become the partitions.

Metadata
''''''''

Once opened, a DataSource object can have arbitrary metadata associated with it.  The metadata for a data source has basically the same data model as a JSON object (to ensure it can be serialized without special measures).  This metadata comes from two sources:

1. A data catalog entry can associate fixed metadata with the data source.  This is helpful for data formats that do not have any support for metadata within the file format.

2. A plugin can add additional metadata when the schema is loaded for the data source.  This allows metadata embedded in the data source to be exported.  

From the user perspective, all of the metadata should be loaded once the data source has loaded the rest of the schema (after ``discover()``, ``read()``, ``to_dask()``, etc have been called).


How to subclass ``intake.source.base.Plugin``
---------------------------------------------


How to subclass ``intake.source.base.DataSource``
-------------------------------------------------