.. _roadmap:

Roadmap
=======

Some high-level work that we expect to be achieved on the time-scale of months. This list
is not exhaustive, but rather aims to whet the appetite for what Intake can be in the future.

Since Intake aims to be a community of data-oriented pythoneers, nothing written here is laid in
stone, and users and devs are encouraged to make their opinions known!


Broaden the coverage of formats
-------------------------------

Data-type drivers are easy to write, but still require some effort, and therefore reasonable
impetus to get the work done. Conversations over the coming months can help determine the
drivers that should be created by the Intake team, and those that might be contributed by the
community.

The next type that we would specifically like to consider is machine learning
model artifacts.  **EDIT** see https://github.com/AlbertDeFusco/intake-sklearn , and
hopefully more to come.

Streaming Source
----------------

Many data sources are inherently time-sensitive and event-wise. These are not covered well by existing
Python tools, but the ``streamz`` library may present a nice way to model them. From the Intake point of
view, the task would be to develop a streaming type, and at least one data driver that uses it.

The most obvious place to start would be read a file: every time a new line appears in the file, an event
is emitted. This is appropriate, for instance, for watching the log files of a web-server, and indeed could
be extended to read from an arbitrary socket.

**EDIT** see: https://github.com/intake/intake-streamz


Server publish hooks
--------------------

To add API endpoints to the server, so that a user (with sufficient privilege) can post data
specifications to a running server, optionally saving the specs to a catalog server-side. Furthermore,
we will consider the possibility of being able to upload and/or transform data
(rather than refer to it in a third-party location), so that you would have a one-line "publish"
ability from the client.

The server, in general, could do with a lot of work to become more than the current
demonstration/prototype. In particular, it should be able to be performant and scalable,
meaning that the server implementation ought to keep as little local state as possible.

Simplify dependencies and class hierarchy
-----------------------------------------

We would like the make it easier to write Intake drivers which don't need any
persist or GUI functionality, and to be able to install Intake core
functionality (driver registry, data loading and catalog traversal) without
needing many other packages at all.

**EDIT** this has been partly done, you can derive from ``DataSourceBase`` and
not have to use the full set of Intake's features for simplicity. We have also gone
some distance to separate out dependencies for parts of the package, so that you
can install Intake and only use some of the subpackages/modules - imports don't
happen until those parts of the code are used. We have *not* yet split the
intake conda package into, for example, intake-base, intake-server, intake-gui...

Reader API
----------

For those that wish to provide Intake's data source API, and make data sources
available to Intake cataloguing, but don't wish to take Intake as a direct dependency.
The actual API of ``DataSources`` is rather simple:

- ``__init__``: collect arguments, minimal IO at this point
- ``discover()``: get metadata from the source, by querying the files/service itself
- ``read()``: return in-memory version of the data
- ``to_*``: return reference objects for the given compute engine, typically Dask
- ``read_partition(...)``: read part of the data into memory, where the argument
  makes sense for the given type of data
- ``configure_new()``: create new instance with different arguments
- ``yaml()``: representation appropriate for inclusion in a YAML catalogue
- ``close()``: release any resources

Of these, only the first three are really necessary for a iminal interface, so
Intake might do well to publish this *protocol specification*, so that new drivers
can be written that can be used by Intake but do not need Intake, and so help
adoption.
