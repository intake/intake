.. _server:

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

GET /info
---------

Retrieve information about the data-sets available on this server. The list of data-sets may be
paginated, in order to avoid excessively long transactions. Notice that the catalog for which a listing
is being requested can itself be a data-source (when ``source-id`` is passed) - this is how nested
sub-catalogs are handled on the server.

Parameters
~~~~~~~~~~

- ``page_size``, int or none (optional): to enable pagination, set this value. The number of entries returned
  will be this value at most. If None, returns all entries. This is passed as a query parameter.

- ``page_offset``, int (optional): when paginating, start the list from this numerical offset. The order of entries
  is guaranteed if the base catalog has not changed. This is passed as a query parameter.

- ``source-id``, uuid string (optional): when the catalog being accessed is not the route catalog, but an open data-source
  on the server, this is its unique identifier. See ``POST /source`` for how these IDs are generated.
  If the catalog being accessed is the root Catalog, this parameter should be omitted. This is passed as an HTTP header.

Returns
~~~~~~~

- ``version``, string: the server's Intake version

- ``sources``, list of objects: the main payload, where each object contains a ``name``, and the result of calling
  ``.describe()`` on the corresponding data-source, i.e., the container type, description, metadata.

- ``metadata``, object: any metadata associated with the whole catalog

GET /source
-----------

Fetch information about a specific source. This is the random-access variant of the ``GET /info`` route, by which
a particular data-source can be accessed without paginating through all of the sources.

Parameters
~~~~~~~~~~

- ``name``, string (required): the data source name being accessed, one of the members of the catalog. This is passed as a query parameter.

- ``source-id``, uuid string (optional): when the catalog being accessed is not the root catalog, but an open data-source
  on the server, this is its unique identifier. See ``POST /source`` for how these IDs are generated.
  If the catalog being accessed is the root Catalog, this parameter should be omitted. This is passed as an HTTP header.

Returns
~~~~~~~

Same as one of the entries in ``sources`` for ``GET /info``: the result of ``.describe()`` on the given data-source in the
server

POST /source, action="search"
-----------------------------

Searching a Catalog returns search results in the form of a new Catalog. This
"results" Catalog is cached on the server the same as any other Catalog.

Parameters
~~~~~~~~~~

- ``source-id``, uuid string (optional): When the catalog being searched is not
  the root catalog, but a subcatalog on the server, this is its unique
  identifier. If the catalog being searched is the root Catalog, this parameter
  should be omitted. This is passed as an HTTP header.
- ``query``: tuple of ``(args, kwargs)``: These will be unpacked into 
  ``Catalog.search`` on the server to create the "results" Catalog. This is passed in the body of the message.

Returns
~~~~~~~

- ``source_id``, uuid string: the identifier of the results Catalog in the
  server's source cache


POST /source, action="open"
---------------------------

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
server and returning its UUID, such that a listing can be made using ``GET/ info`` or ``GET /source``.

Parameters
~~~~~~~~~~

- ``name``, string (required): the data source name being accessed, one of the members of the catalog. This is passed in the body of the request.

- ``source-id``, uuid string (optional): when the catalog being accessed is not the root catalog, but an open data-source
  on the server, this is its unique identifier. If the catalog being accessed is the root Catalog, this parameter should be omitted. This
  is passed as an HTTP header.

- ``available_plugins``, list of string (optional): the set of named data drivers supported by the client. If the driver required
  by the data-source is not supported by the client, then the source must be opened remote-access. This is passed in the body of the request.

- ``parameters``, object (optional): user parameters to pass to the data-source when instantiating. Whether or not direct-access
  is possible may, in principle, depend on these parameters, but this is unlikely. Note that some parameter default
  value functions are designed to be evaluated on the server, which may have access to, for example, some credentials
  service (see :ref:`paramdefs`). This is passed in the body of the request.

Returns
~~~~~~~

If direct-access, the driver plugin name and set of arguments for instantiating the data-soruce in the client.

If remote-access, the data-source container, schema and source-ID so that further reads can be made from the
server.

POST /source, action="read"
---------------------------

This route fetches data from the server once a data-source has been opened in remote-access mode.

Parameters
~~~~~~~~~~
- ``source-id``, uuid string (required): the identifier of the data-source in the server's source cache. This is returned
  when ``action="open"``. This is passed in the body of the request.

- ``partition``, int or tuple (optional, but necessary for some sources): section/chunk of the data to fetch.
  In cases where the data-source is partitioned,
  the client will fetch the data one partition at a time, so that it will appear partitioned in the same manner on
  the client side for iteration of passing to Dask. Some data-sources do not support partitioning, and then this
  parameter is not required/ignored. This is passed in the body of the request.

- ``accepted_formats``, ``accepted_compression``, list of strings (required): to specify how serialization of data happens. This
  is an expert feature, see docs in the module ``intake.container.serializer``. This is passed in the body of the request.
