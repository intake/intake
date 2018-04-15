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
* python (list of Python objects, usually dictionaries)

A given plugin must only return one kind of container.  If a file format (such as HDF5) could reasonably be interpreted as two different data models depending on usage (such as a dataframe or an ndarray), then two different plugins need to be created with different names.  If a plugin returns the ``python`` container, it should document what Python objects will appear in the list.

The source of data should be essentially permanent and immutable.  That is, loading the data should not destroy or modify the data, nor should closing the data source destroy the data either.  When a data source is serialized and sent to another host, it will need to be reopened at the destination, which may cause queries to be re-executed and files to be reopened.  Data sources that treat readers as "consumers" and remove data once read will cause erratic behavior, so Intake is not suitable for accessing things like FIFO message queues.

Schema
''''''

The schema of a data source is assumed to be knowable prior to loading the data in full.  However, different kinds of data sources may not be able to determine their schema entirely ahead of time.  For example, determining the length of a CSV data set requires scanning the constituent files, and many databases will not report the field names and field types of the result set until *after* executing the query.  In these cases, there may be unknown parts of the schema before the read is executed.  Plugins may choose to have the user input this unknown information in the `open()` method, or do some kind of partial data inspection to determine the schema.  Regardless of method used, the time spent figuring out the schema ahead of time should be short and not scale with the size of the data.

Partitioning
''''''''''''

Data sources are assumed to be *partitionable*.  A data partion is a randomly accessible fragment of the data.  Partitions are numbered, starting from zero, and correspond to contiguous chunks of data divided along the first dimension of the data structure.  (Tables and Python lists only have one dimension, ndarrays can have more than one.)

.. note::

    Representation of ndarrays partitioned along multiple axes is TBD, but a desired feature.

Not all data sources can be partitioned.  For example, arbitrary SQL queries generally cannot be partitioned, and file formats without sufficient indexing often can only be read from beginning to end.  In these cases, the DataSource object should report that there is only 1 partition.  However, it often makes sense for a data source to be able to represent a directory of files, in which case each file will correspond to one partition.

Metadata
''''''''

Once opened, a DataSource object can have arbitrary metadata associated with it.  The metadata for a data source should be a dictionary that can be serialized as JSON.  This metadata comes from two sources:

1. A data catalog entry can associate fixed metadata with the data source.  This is helpful for data formats that do not have any support for metadata within the file format.

2. A plugin can add additional metadata when the schema is loaded for the data source.  This allows metadata embedded in the data source to be exported.  

From the user perspective, all of the metadata should be loaded once the data source has loaded the rest of the schema (after ``discover()``, ``read()``, ``to_dask()``, etc have been called).


How to subclass ``intake.source.base.Plugin``
---------------------------------------------

Every Intake plugin class should be a subclass of ``intake.source.base.Plugin``.  The plugin subclass only needs to implement the ``__init__()`` and ``open()`` methods, as shown in the example below::

    class FooPlugin(base.Plugin):
        def __init__(self):
            '''Create new instance of the plugin for the Foo file format.
            super(Plugin, self).__init__(name='foo', version='0.1',
                                        container='dataframe',
                                        partition_access=True)

        def open(self, urlpath, **kwargs):
            '''Return a data source for Foo-format file with filename ``urlpath``.'''
            base_kwargs, source_kwargs = self.separate_base_kwargs(kwargs)
            return FooSource(urlpath=urlpath, my_kwargs=source_kwargs,
                            metadata=base_kwargs['metadata'])

The ``__init__()`` method should take no arguments but needs to call the base ``__init__()`` to set some key attributes for the plugin:

- ``name``: The short name of the plugin.  This should be a valid python identifier.  You should not include the word ``intake`` in the plugin name.
- ``version``: A version string for the plugin.  This may be reported to the user by tools based on Intake, but has no semantic importance.
- ``container``: The container type of data sources created by this object.  Currently this must be one of ``dataframe``, ``ndarray``, or ``python``.  For simplicity, a plugin many only return one typed of container.  If a particular source of data could be used in multiple ways (such as HDF5 files interpreted as dataframes or as ndarrays), two plugins must be created.  These two plugins can be part of the same Python package.
-  ``partition_access``: Do the data sources returned by this plugin have multiple partitions?  This may help tools in the future make more optimal decisions about how to present data.  If in doubt (or the answer depends on arguments to ``open()``), ``True`` will always result in correct behavior, even if the data source has only one partition.

The ``open()`` method should return an instance of a subclass of ``intake.source.base.DataSource`` (see below).  The arguments of the ``open()`` method are entirely up to the plugin, except some standard optional arguments used by catalogs:

- ``metadata``: a dictionary of metadata from the catalog to associate with the source.  This dictionary must be serializable as JSON.

To help with ``open()`` methods that need to use ``**kwargs`` for other reasons, the base class includes a ``separate_base_kwargs`` method which will split out the above arguments from the rest into separate dictionaries.

Note that the plugin class will always be instantiated before use, but is not guaranteed to be a singleton.  It may be instantiated into several different plugin registries.  Use of mutable class attributes is therefore discouraged.

How to subclass ``intake.source.base.DataSource`` (easy way)
------------------------------------------------------------

We encourage most plugin authors to subclass `DataSource` and implement only a small number of internal methods::

    class FooSource(intake.source.base.DataSource):
        def __init__(self, a, b, metadata=None):
            # Do init here with a and b
            super(FooSource, self).__init__(
                container='dataframe',
                metadata=metadata
            )

        def _get_schema(self):
            return intake.source.base.Schema(
                datashape='datashape',
                dtype=np.dtype([('x', np.int64), ('y', np.int64)]),
                shape=(6,),
                npartitions=2,
                extra_metadata=dict(c=3, d=4)
            )

        def _get_partition(self, i):
            # Return the appropriate container of data here
            return pd.DataFrame({'x': [1, 2, 3], 'y': [10, 20, 30]})

        def _close(self):
            # close any files, sockets, etc
            pass

After implementing these 4 methods, the base class will provide the expected behavior for all the public DataSource methods.  The implementations of these internal methods should follow the following rules:

- ``__init__(self)``: Should be very lightweight and fast.  No files or network resources should be opened, and no signifcant memory should be allocated yet.  Data sources are often serialized immediately.  The default implementation of the pickle protocol in the base class will record all the arguments to ``__init__()`` and recreate the object with those arguments when unpickled, assuming the class has no side effects.
- ``_get_schema(self)``: May open files and network resources and return as much of the schema as possible in small amount of *approximately* constant  time.  The ``npartitions`` and ``extra_metadata`` attributes must be correct when ``_get_schema`` returns.  The ``dtype`` attribute is next most important, but may be ``None`` if unknown.  The ``shape`` tuple can contain ``None`` values if the size of some dimensions is not known before reading.  The ``datashape`` attribute is a placeholder for future integration with Blaze-like systems and optional for now.
- ``_get_partition(self, i)``: Should return all of the data from partition id ``i``, where ``i`` is an integer.  The base class will automatically verify that ``i`` is in the range ``[0, npartitions)``, so no range checking is required.
- ``_close(self)``: Close any network or file handles and deallocate any significant memory.  Note that these resources may be need to be reopened/reallocated if ``_get_partition()`` is called again later.


How to subclass ``intake.source.base.DataSource`` (harder way)
--------------------------------------------------------------

Some plugins may benefit from taking full control of the DataSource behavior, in which case they will need to override all the public methods, rather than the internal methods described in the previous section.  The implementations should follow these rules:

- ``__init__(self)``: Same as above.  The standard object attributes (like ``dtype`` and ``shape``) should be set to default placeholder values (``None``) if they are not known yet. 
- ``discover(self)``: Read the source attributes, like ``npartitions``, etc.  As with ``_get_schema()`` above, this method is assumed to be fast, and make a best effort to set attributes.
- ``read(self)``: Return all the data in memory in one in-memory container.
- ``read_chunked(self)``: Return an iterator that returns contiguous chunks of the data.  The chunking is generally assumed to be at the partition level, but could be finer grained if desired.
- ``read_partition(self, i)``: Returns the data for a given partition id.  It is assumed that reading a given partition does not require reading the data that precedes it.  If ``i`` is out of range, an ``IndexError`` should be raised.
- ``to_dask(self)``: Return a (lazy) Dask data structure corresponding to this data source.  It should be assumed that the data can be read from the Dask workers, so the loads can be done in future tasks.  For further information, see the `Dask documentation <https://dask.pydata.org/en/latest/>`_.  
- ``close(self)``: Close network or file handles and deallocate memory.  If other methods are called after ``close()``, the source is automatically reopened.

It is also important to note that source attributes should be set after ``read()``, ``read_chunked()``, ``read_partition()`` and ``to_dask()``, even if ``discover()`` was not called by the user.

.. _plugin-discovery:

Plugin Discovery
----------------

When Intake is imported, it will search the Python module path (by default includes ``site-packages`` and other directories in your ``$PYTHONPATH``) for packages starting with ``intake_`` and discover Plugin subclasses inside those packages to register.  Plugins will be registered based on the``name`` attribute of the object after being instantiated, *not* the name of the Plugin class or module.  By convention, plugins should have names that are lowercase, valid Python identifiers that do not contain the word ``intake``.

After the discovery phase, Intake will automatically create ``open_[plugin_name]`` convenience functions under the ``intake`` module namespace.  Calling a function like ``open_csv()`` is equivalent to calling the ``open()`` method on the ``csv`` plugin.

To take advantage of plugin discovery, give your installed package a name that starts with ``intake_`` and define your plugin class(es) in the ``__init__.py`` of the package.

Remote Data
-----------

For plugins loading from files, the author should be aware that it is easy to implement loading
from files stored in remote services. A simplistic case is demonstrated by the included CSV plugin,
which simply passes a URL to Dask, which in turn can interpret the URL as a remote data service,
and use the ``storage_options`` as required (see the Dask documentation on `remote data`_).

.. _remote data: http://dask.pydata.org/en/latest/remote-data-services.html

More advanced usage, where a Dask loader does not already exist, will likely rely on
`dask.bytes.open_files`_ . Use this function to produce lazy ``OpenFile`` object for local
or remote data, based on a URL, which will have a protocol designation and possibly contain
glob "*" characters. Additional parameters may be passed to ``open_files``, which should,
by convention, be supplied by a plugin argument named ``storage_options`` (a dictionary).

.. _dask.bytes.open_files: http://dask.pydata.org/en/latest/bytes.html#dask.bytes.open_files

To use an ``OpenFile`` object, make it concrete by using a context:


.. code-block::python

    # at setup, to discover the number of files/partitions
    set_of_open_files = dask.bytes.open_files(urlpath, mode='rb', **storage_options)

    # when actually loading data; here we loop over all files, but maybe we just do one partition
    for an_open_file in set_of_open_files:
        # `with` causes the object to become concrete until the end of the block
        with an_open_file as f:
            # do things with f, which is a file-like object
            f.seek(); f.read()
