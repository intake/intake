Server Protocol
===============

This page gives deeper details on how the Intake :term:`server` is implemented. For those
simply wishing to run and configure a server, see the :doc:`tools` section.

Communication between the intake client and server happens exclusively over HTTP, with all
parameters passed using msgpack UTF8 encoding. The
server side is implemented by the module ``intake.cli.server``. Currently, only the following
two routes are available:

   - ``http://server:port/v1/info``
   - ``http://server:port/v1/source``.

The server may be configured to use auth services, which, when passed the header of the incoming
call, can determine whether the given request is allowed. See :doc:`auth-plugins`.

Info handler, GET
-----------------

Retrieve information about the data-sets available on this server. The list of data-sets may be
paginated, in order to avoid excessively long transactions. Notice that the catalog for which a listing
is being requested can itself be a data-source (when ``source_id`` is passed) - this is how nested
sub-catalogs are handled on the server.

Parameters
~~~~~~~~~~

- ``page_size``, int or none: to enable pagination, set this value. The number of entries returned
  will be this value at most. If None, returns all entries.

- ``page_offset``, int: when paginating, start the list from this numerical offset. The order of entries
  is guaranteed if the base catalog has not changed.

- ``source_id``, uuid string: when the catalog being accessed is not the route catalog, but an open data-source
  on the server, this its unique identifier. See Source, POST for how these IDs are generated.

Returns
~~~~~~~

- ``version``, string: the server's Intake version

- ``sources``, list of objects: the main payload, where each object contains a ``name``, and the result of calling
  ``.describe()`` on the corresponding data-source, i.e., the container type, description, metadata.

- ``metadata``, object: any metadata associated with the whole catalog

Source handler, GET
-------------------

Fetch information about a specific source. This is the random-access variant of the Info, GET route, by which
a particular data-source can be accessed without paginating through all of the sources.

Parameters
~~~~~~~~~~

- ``name``, string: the data source name being accessed, one of the members of the catalog

- ``source_id``, uuid string: when the catalog being accessed is not the root catalog, but an open data-source
  on the server, this its unique identifier. See Source, POST for how these IDs are generated.

Returns
~~~~~~~

Same as one of the entries in ``sources`` for Info, GET: the result of ``.describe()`` on the given data-source in the
server

Source handler, POST, action="open"
-----------------------------------

This is a more involved processing of a data-source, and, if successful, returns one of two possible scenarios:

- direct-access, in which all the details required for reading the data directly from the client are passed, and
  the client then creates a local copy of the data source and needs no further involvement from the server in order
  to fetch the data

- remote-access, in which the client is unable or unwilling to create a local version of the data-source, and instead
  created a remote data-source which will fetch the data for each partition from the server.

The set of parameters supplied and the server/client policies will define which method of access is employed. In the
case of remote-access, the data source is instantiated on the server, and ``.discover()`` run on it. The resulting
information is passed back, and must be enough to instantiate a subclass of ``intake.container.base.RemoteSource``
appropriate for the container of the data-set in question (e.g., ``RemoteArray`` when ``container="ndarray"``).
In this case, the response also includes a UUID string for the open instance on the server, referencing the
cache of open sources maintained by the server.

Note that "opening" a data entry which is itself is a catalog implies instantiating that catalog object on the
server and returning its UUID, such that a listing can be made using Info, GET or Source, GET.

Parameters
~~~~~~~~~~

- ``name``, string: the data source name being accessed, one of the members of the catalog

- ``source_id``, uuid string: when the catalog being accessed is not the root catalog, but an open data-source
  on the server, this its unique identifier.

- ``available_plugins``, list of string: the set of named data drivers supported by the client. If the driver required
  by the data-source is not supported by the client, then the source must be opened remote-access.

- ``parameters``, object: user parameters to pass to the data-source when instantiating. Whether or not direct-access
  is possible may, in principle, depend on these parameters, but this is unlikely. Note that some parameter default
  value functions are designed to be evaluated on the server, which may have access to, for example, some credentials
  service (see :ref:`paramdefs`).

Returns
~~~~~~~

If direct-access, the driver plugin name and set of arguments for instantiating the data-soruce in the client.

If remote-access, the data-source container, schema and source-ID so that further reads can be made from the
server.

Source handler, POST, action="read"
-----------------------------------

This route fetches data from the server once a data-source has been opened in remote-access mode.

Parameters
~~~~~~~~~~
- ``source_id``, uuid string: the identifier of the data-source in the server's source cache. This is returned
  when ``action="open"``.

- ``partition``, int or tuple: section/chunk of the data to fetch. In cases where the data-source is partitioned,
  the client will fetch the data one partition at a time, so that it will appear partitioned in the same manner on
  the client side for iteration of passing to Dask. Some data-sources do not support partitioning, and then this
  parameter is not required/ignored.

- ``accepted_formats``, ``accepted_compression``, list of strings: to specify how serialization of data happens. This
  is an expert feature, see docs in the module ``intake.container.serializer``.
