.. _persisting:

Persisting Data
===============

(this is an experimental new feature, expect enhancements and changes)

Introduction
------------

As defined in the glossary, to :term:`Persist` is to convert data into the storage format
most appropriate for the container type, and save a copy of this for rapid lookup in the future.
This is of great potential benefit where the creation or transfer of the original data source
takes some time.

This is not to be confused with the file :term:`Cache`.

Usage
-----

Any :term:`Data Source` has a method ``.persist()``. The only option that you will need to
pick is a :term:`TTL`, the number of seconds that the persisted version lasts before
expiry (leave as ``None`` for no expiry). This creates a local copy in the persist
directory, which may be in ``"~/.intake/persist``, but can be configured.

Each container type (dataframe, array, ...) will have its own implementation of persistence,
and a particular file storage format associated. The call to ``.persist()`` may take
arguments to tune how the local files are created, and in some cases may require additional
optional packages to be installed.

Example::

    cat = intake.load_catalog('mycat.yaml')  # load a remote cat
    source = cat.csvsource()  # source pointing to remote data
    source.persist()

    source = cat.csvsource()  # future use now gives local intake_parquet.ParquetSource

To control whether a catalog will automatically give you the persisted version of a
source in this way using the argument ``persist_mode``, e.g., to ignore locally
persisted versions, you could have done::

    cat = intake.load_catalog('mycat.yaml', persist_mode='never')
    or
    source = cat.csvsource(persist_mode='never')

Note that if you give a TTL (in seconds), then the original source will be accessed
and a new persisted version written transparently when the old persisted version has expired.

Note that after persisting, the original source will have ``source.has_been_persisted == True``
and the persisted source (i.e., the one loaded from local files) will have
``source.is_persisted == True``.

The Persist Store
-----------------

You can interact directly with the class implementing persistence::

    from intake.container.persist import store

This singleton instance, which acts like a catalog, allows you to query the contents of the
instance store and to add and remove entries. It also allows you to find the original
source for any given persisted source, and refresh the persisted version on demand.

For details on the methods of the persist store, see the API documentation:
:func:`intake.container.persist.PersistStore`. Sources in the store carry a lot of
information about the sources they were made from, so that they can be remade
successfully. This all appears in the source metadata.
The sources use the "token" of the original
data source as their key in the store, a value which can be found by ``source._tok``
for the original source, or can be taken from the metadata of a persisted source.

Future Enhancements
-------------------

- Implement persist for all data types, including catalogs, which have a natural representation
  as YAML files - fetching or querying a remote service to create a catalog can be slow too.

- CLI functionality to investigate and alter the state of the persist store, similar to the
  cache commands.

- Being able to set the persist store to a remote URL, such that outputs can be shared between
  users and the nodes of a Dask cluster. Currently, the only way to achieve this would be to
  use a shared network file-system.

- An "export" function much like persist, but producing a standalone version of the data together
  with a catalog spec for it, so that it can be easily shared.

- Time check-pointing of persisted data, such that you can not only get the "most recent" but
  any version in the time-series.

- (eventually) pipeline functionality, whereby a persisted data source depends on another
  persisted data source, and the whole train can be refreshed on a schedule or on demand.