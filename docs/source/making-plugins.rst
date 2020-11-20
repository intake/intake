Making Drivers
==============

The goal of the Intake plugin system is to make it very simple to implement a :term:`Driver` for a new data source, without
any special knowledge of Dask or the Intake catalog system.

Assumptions
-----------

Although Intake is very flexible about data, there are some basic assumptions that a driver must satisfy.

Data Model
''''''''''

Intake currently supports 3 kinds of containers, represented the most common data models used in Python:

* dataframe
* ndarray
* python (list of Python objects, usually dictionaries)

Although a driver can load *any* type of data into any container, and new container types can be added to the list
above, it is reasonable to expect that the number of container types remains small. Declaring a container type is
only informational for the user when read locally, but streaming of data from a server requires that the container type
be known to both server and client.

A given driver must only return one kind of container.  If a file format (such as HDF5) could reasonably be
interpreted as two different data models depending on usage (such as a dataframe or an ndarray), then two different
drivers need to be created with different names.  If a driver returns the ``python`` container, it should document
what Python objects will appear in the list.

The source of data should be essentially permanent and immutable.  That is, loading the data should not destroy or
modify the data, nor should closing the data source destroy the data either.  When a data source is serialized and
sent to another host, it will need to be reopened at the destination, which may cause queries to be re-executed and
files to be reopened.  Data sources that treat readers as "consumers" and remove data once read will cause erratic
behavior, so Intake is not suitable for accessing things like FIFO message queues.

Schema
''''''

The schema of a data source is a detailed description of the data, which can be known by loading only metadata or by
loading only some small representative portion of the data. It is information to present to the user about the data
that they are considering loading, and may be important in the case of server-client communication. In the latter
context, the contents of the schema must be serializable by ``msgpack`` (i.e., numbers, strings, lists and
dictionaries only).

There may be unknown parts of
the schema before the whole data is read.  drivers may require this unknown information in the
`__init__()` method (or the catalog spec), or do some kind of partial data inspection to determine the schema; or
more simply, may be given as unknown ``None`` values.
Regardless of method used, the
time spent figuring out the schema ahead of time should be short and not scale with the size of the data.

Typical fields in a schema dictionary are ``npartitions``, ``dtype``, ``shape``, etc., which will be more appropriate
for some drivers/data-types than others.

Partitioning
''''''''''''

Data sources are assumed to be *partitionable*.  A data partition is a randomly accessible fragment of the data.
In the case of sequential and data-frame sources, partitions are numbered, starting from zero, and correspond to
contiguous chunks of data divided along the first
dimension of the data structure. In general, any partitioning scheme is conceivable, such as a tuple-of-ints to
index the chunks of a large numerical array.

Not all data sources can be partitioned.  For example, file
formats without sufficient indexing often can only be read from beginning to end.  In these cases, the DataSource
object should report that there is only 1 partition.  However, it often makes sense for a data source to be able to
represent a directory of files, in which case each file will correspond to one partition.

Metadata
''''''''

Once opened, a DataSource object can have arbitrary metadata associated with it.  The metadata for a data source
should be a dictionary that can be serialized as JSON.  This metadata comes from the following sources:

1. A data catalog entry can associate fixed metadata with the data source.  This is helpful for data formats that do
   not have any support for metadata within the file format.

2. The driver handling the data source may have some general metadata associated with the state of the system at the
   time of access, available even before loading any data-specific information.

2. A driver can add additional metadata when the schema is loaded for the data source.  This allows metadata embedded
   in the data source to be exported.

From the user perspective, all of the metadata should be loaded once the data source has loaded the rest of the
schema (after ``discover()``, ``read()``, ``to_dask()``, etc have been called).


Subclassing ``intake.source.base.DataSourceBase``
-------------------------------------------------

Every Intake driver class should be a subclass of ``intake.source.base.DataSource``.
The class should have the following attributes to identify itself:

- ``name``: The short name of the driver.  This should be a valid python identifier.
  You should not include the
  word ``intake`` in the driver name.

- ``version``: A version string for the driver.  This may be reported to the user by tools
  based on Intake, but has
  no semantic importance.

- ``container``: The container type of data sources created by this object, e.g.,
  ``dataframe``, ``ndarray``, or
  ``python``, one of the keys of ``intake.container.container_map``.
  For simplicity, a driver many only return one typed of container.  If a particular
  source of data could
  be used in multiple ways (such as HDF5 files interpreted as dataframes or as ndarrays),
  two drivers must be created.
  These two drivers can be part of the same Python package.

- ``partition_access``: Do the data sources returned by this driver have multiple
  partitions?  This may help tools in
  the future make more optimal decisions about how to present data.  If in doubt
  (or the answer depends on init
  arguments), ``True`` will always result in correct behavior, even if the data
  source has only one partition.

The ``__init()__`` method should always accept a keyword argument ``metadata``, a
dictionary of metadata from the
catalog to associate with the source.  This dictionary must be serializable as JSON.

The `DataSourceBase` class has a small number of methods which should be overridden.
Here is an example producing a
data-frame::

    class FooSource(intake.source.base.DataSource):
        container = 'dataframe'
        name = 'foo'
        version = '0.0.1'
        partition_access = True

        def __init__(self, a, b, metadata=None):
            # Do init here with a and b
            super(FooSource, self).__init__(
                metadata=metadata
            )

        def _get_schema(self):
            return intake.source.base.Schema(
                datashape=None,
                dtype={'x': "int64", 'y': "int64"},
                shape=(None, 2),
                npartitions=2,
                extra_metadata=dict(c=3, d=4)
            )

        def _get_partition(self, i):
            # Return the appropriate container of data here
            return pd.DataFrame({'x': [1, 2, 3], 'y': [10, 20, 30]})

        def read(self):
            self._load_metadata()
            return pd.concat([self.read_partition(i) for i in range(self.npartitions)])

        def _close(self):
            # close any files, sockets, etc
            pass

Most of the work typically happens in the following methods:

- ``__init__()``: Should be very lightweight and fast.  No files or network resources should be opened, and no
  significant memory should be allocated yet.  Data sources might be serialized immediately.  The default implementation
  of the pickle protocol in the base class will record all the arguments to ``__init__()`` and recreate the object with
  those arguments when unpickled, assuming the class has no side effects.

- ``_get_schema()``: May open files and network resources and return as much of the schema as possible in small
  amount of *approximately* constant  time. Typically, imports of packages needed by the source only happen here.
  The ``npartitions`` and ``extra_metadata`` attributes must be correct
  when ``_get_schema`` returns.  Further keys such as ``dtype``, ``shape``, etc., should reflect the container type of
  the data-source, and can be ``None`` if not easily knowable, or include ``None`` for some elements. File-based
  sources should use fsspec to open a local or remote URL, and pass ``storage_options`` to it. This ensures
  compatibility and extra features such as caching. If the backend can only deal with local files, you may
  still want to use ``fsspec.open_local`` to allow for caching.

- ``_get_partition(self, i)``: Should return all of the data from partition id ``i``, where ``i`` is typically an
  integer, but may be something more complex.
  The base class will automatically verify that ``i`` is in the range ``[0, npartitions)``, so no range checking is
  required in the typical case.

- ``_close(self)``: Close any network or file handles and deallocate any significant memory.  Note that these
  resources may be need to be reopened/reallocated if a read is called again later.

The full set of user methods of interest are as follows:

- ``discover(self)``: Read the source attributes, like ``npartitions``, etc.  As with ``_get_schema()`` above, this
  method is assumed to be fast, and make a best effort to set attributes. The output should be serializable, if the
  source is to be used on a server; the details contained will be used for creating a remote-source on the client.

- ``read(self)``: Return all the data in memory in one in-memory container.

- ``read_chunked(self)``: Return an iterator that returns contiguous chunks of the data.  The chunking is generally
  assumed to be at the partition level, but could be finer grained if desired.

- ``read_partition(self, i)``: Returns the data for a given partition id.  It is assumed that reading a given
  partition does not require reading the data that precedes it.  If ``i`` is out of range, an ``IndexError`` should
  be raised.

- ``to_dask(self)``: Return a (lazy) Dask data structure corresponding to this data source.  It should be assumed
  that the data can be read from the Dask workers, so the loads can be done in future tasks.  For further information,
  see the `Dask documentation <https://dask.pydata.org/en/latest/>`_.

- ``close(self)``: Close network or file handles and deallocate memory.  If other methods are called after ``close()``,
  the source is automatically reopened.

- ``to_*``: for some sources, it makes sense to provide alternative outputs aside from the base container
  (dataframe, array, ...) and Dask variants.

Note that all of these methods typically call ``_get_schema``, to make sure that the source has been
initialised.

Subclassing ``intake.source.base.DataSource``
---------------------------------------------

``DataSource`` provides the same functionality as ``DataSourceBase``, but has some additional mixin
classes to provide some extras. A developer may choose to derive from ``DataSource`` to get all of
these, or from ``DataSourceBase`` and make their own choice of mixins to support.

- ``HoloviewsMixin``: provides plotting and GUI capabilities via the `holoviz`_ stack

- ``PersistMixin``: allows for storing a local copy in a default format for the given
  container type

- ``CacheMixin``: allows for local storage of data files for a source. Deprecated,
  you should use one of the caching mechanisms in ``fsspec``.

.. _holoviz: https://holoviz.org/index.html

.. _driver-discovery:

Driver Discovery
----------------

Intake discovers available drivers in three different ways, described below.
After the discovery phase, Intake will automatically create
``open_[driver_name]`` convenience functions under the ``intake`` module
namespace.  Calling a function like ``open_csv()`` is equivalent to
instantiating the corresponding data-source class.

Entrypoints
'''''''''''

If you are packaging your driver into an installable package to be shared, you
should add the following to the package's ``setup.py``:

.. code-block:: python

   setup(
       ...
       entry_points={
           'intake.drivers': [
               'some_format_name = some_package.and_maybe_a_submodule:YourDriverClass',
               ...
           ]
       },
   )

.. important::

   Some critical details of Python's entrypoints feature:

   * Note the unusual syntax of the entrypoints. Each item is given as one long
     string, with the ``=`` as part of the string. Modules are separated by
     ``.``, and the final object name is preceded by ``:``.
   * The right hand side of the equals sign must point to where the object is
     *actually defined*. If ``YourDriverClass`` is defined in
     ``foo/bar.py`` and imported into ``foo/__init__.py`` you might expect
     ``foo:YourDriverClass`` to work, but it does not. You must spell out
     ``foo.bar:YourDriverClass``.

Entry points are a way for Python packages to advertise objects with some
common interface. When Intake is imported, it discovers all packages installed
in the current environment that advertise ``'intake.drivers'`` in this way.

Most packages that define intake drivers have a dependency on ``intake``
itself, for example in order to use intake's base classes. This can create a
ciruclar dependency: importing the package imports intake, which tries
to discover and import packages that define drivers. To avoid this pitfall,
just ensure that ``intake`` is imported first thing in your package's
``__init__.py``. This ensures that the driver-discovery code runs first. Note
that you are *not* required to make your package depend on intake. The rule is
that *if* you import ``intake`` you must import it first thing. If you do not
import intake, there is no circularity.

Configuration
'''''''''''''

The intake configuration file can be used to:

* Specify precedence in the event of name collisions---for example, if two different
  ``csv`` drivers are installed.
* Disable a troublesome driver.
* Manually make intake aware of a driver, which can be useful for
  experimentation and early development until a ``setup.py`` with an
  entrypoint is prepared.
* Assign a driver to a name other than the one assigned by the driver's
  author.

The commandline invocation

.. code-block:: bash

   intake drivers enable some_format_name some_package.and_maybe_a_submodule.YourDriverClass

is equivalent to adding this to your intake configuration file:

.. code-block:: yaml

   drivers:
     some_format_name: some_package.and_maybe_a_submodule.YourDriverClass

You can also disable a troublesome driver

.. code-block:: bash

   intake drivers disable some_format_name

which is equivalent to

.. code-block:: yaml

   drivers:
     your_format_name: false

Deprecated: Package Scan
''''''''''''''''''''''''

When Intake is imported, it will search the Python module path (by default includes ``site-packages`` and other
directories in your ``$PYTHONPATH``) for packages starting with ``intake\_`` and discover DataSource subclasses inside
those packages to register.  drivers will be registered based on the``name`` attribute of the object.
By convention, drivers should have names that are lowercase, valid Python identifiers that do not contain the word
``intake``.

This approach is deprecated because it is limiting (requires the package to
begin with "intake\_") and because the package scan can be slow. Using
entrypoints is strongly encouraged. The package scan *may* be disabled by
default in some future release of intake. During the transition period, if a
package named ``intake_*`` provides an entrypoint for a given name, that will
take precedence over any drivers gleaned from the package scan having that
name. If intake discovers any names from the package scan for which there are
no entrypoints, it will issue a ``FutureWarning``.

Python API to Driver Discovery
''''''''''''''''''''''''''''''

.. autofunction:: intake.source.discovery.autodiscover
.. autofunction:: intake.source.discovery.enable
.. autofunction:: intake.source.discovery.disable

.. _remote_data:

Remote Data
-----------

For drivers loading from files, the author should be aware that it is easy to implement loading
from files stored in remote services. A simplistic case is demonstrated by the included CSV driver,
which simply passes a URL to Dask, which in turn can interpret the URL as a remote data service,
and use the ``storage_options`` as required (see the Dask documentation on `remote data`_).

.. _remote data: http://dask.pydata.org/en/latest/remote-data-services.html

More advanced usage, where a Dask loader does not already exist, will likely rely on
`fsspec.open_files`_ . Use this function to produce lazy ``OpenFile`` object for local
or remote data, based on a URL, which will have a protocol designation and possibly contain
glob "*" characters. Additional parameters may be passed to ``open_files``, which should,
by convention, be supplied by a driver argument named ``storage_options`` (a dictionary).

.. _fsspec.open_files: https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.open_files

To use an ``OpenFile`` object, make it concrete by using a context:


.. code-block:: python

    # at setup, to discover the number of files/partitions
    set_of_open_files = fsspec.open_files(urlpath, mode='rb', **storage_options)

    # when actually loading data; here we loop over all files, but maybe we just do one partition
    for an_open_file in set_of_open_files:
        # `with` causes the object to become concrete until the end of the block
        with an_open_file as f:
            # do things with f, which is a file-like object
            f.seek(); f.read()

The ``textfiles`` builtin drivers implements this mechanism, as an example.


Structured File Paths
---------------------

The CSV driver sets up an example of how to gather data which is encoded in file paths
like (``'data_{site}_.csv'``) and return that data in the output.
Other drivers could also follow the same structure where data is being loaded from a
set of filenames. Typically this would apply to data-frame output.
This is possible as long as the driver has access to each of the file paths at some
point in ``_get_schema``. Once the file paths are known, the driver developer can use the helper
functions defined in ``intake.source.utils`` to get the values for each field in the pattern
for each file in the list. These values should then be added to the data, a process which
normally would happen within the _get_schema method.

The PatternMixin defines driver properties such as urlpath, path_as_pattern, and pattern.
The implementation might look something like this::

    from intake.source.utils import reverse_formats

    class FooSource(intake.source.base.DataSource, intake.source.base.PatternMixin):
        def __init__(self, a, b, path_as_pattern, urlpath, metadata=None):
            # Do init here with a and b
            self.path_as_pattern = path_as_pattern
            self.urlpath = urlpath

            super(FooSource, self).__init__(
                container='dataframe',
                metadata=metadata
            )
        def _get_schema(self):
            # read in the data
            values_by_field = reverse_formats(self.pattern, file_paths)
            # add these fields and map values to the data
            return data


Since dask already has a specific method for including the file paths in the output dataframe,
in the CSV driver we set ``include_path_column=True``, to get a dataframe where one of the
columns contains all the file paths. In this case, `add these fields and values to data`
is a mapping between the categorical file paths column and the ``values_by_field``.

In other drivers where each file is read in independently the driver developer
can set the new fields on the data from each file before concattenating.
This pattern looks more like::

    from intake.source.utils import reverse_format

    class FooSource(intake.source.base.DataSource):
        ...

        def _get_schema(self):
            # get list of file paths
            for path in file_paths:
                # read in the file
                values_by_field = reverse_format(self.pattern, path)
                # add these fields and values to the data
            # concatenate the datasets
            return data


To toggle on and off this path as pattern behavior, the CSV and intake-xarray drivers
uses the bool ``path_as_pattern`` keyword argument.
