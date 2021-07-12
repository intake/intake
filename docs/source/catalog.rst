Catalogs
========

Data catalogs provide an abstraction that allows you to externally define, and optionally share, descriptions of
datasets, called *catalog entries*.  A catalog entry for a dataset includes information like:

* The name of the Intake driver that can load the data
* Arguments to the ``__init__()`` method of the driver
* Metadata provided by the catalog author (such as field descriptions and types, or data provenance)

In addition, Intake allows the arguments to data sources to be templated, with the variables explicitly
expressed as "user parameters". The given arguments are rendered using ``jinja2``, the
values of named user parameterss, and any overrides.
The parameters are also offer validation of the allowed types and values, for both the template
values and the final arguments passed to the data source. The parameters are named and described, to
indicate to the user what they are for. This kind of structure can be used to, for example,
choose between two parts of a given data source, like "latest" and "stable", see the `entry1_part` entry in
the example below.

The user of the catalog can always override any template or argument value at the time
that they access a give source.

The Catalog class
-----------------

In Intake, a ``Catalog`` instance is an object with one or more named entries.
The entries might be read from a static file (e.g., YAML, see the next section), from
an Intake server or from any other data service that has a driver. Drivers which
create catalogs are ordinary DataSource classes, except that they have the container
type "catalog", and do not return data products via the ``read()`` method.

For example, you might choose to instantiate the base class and fill in some entries
explicitly in your code

.. code-block:: python

    from intake.catalog import Catalog
    from intake.catalog.local import LocalCatalogEntry
    mycat = Catalog.from_dict({
        'source1': LocalCatalogEntry(name, description, driver, args=...),
        ...
        })

Alternatively, subclasses of ``Catalog`` can define how entries are created from
whichever file format or service they interact with, examples including ``RemoteCatalog``
and `SQLCatalog`_. These generate entries based on their respective targets; some
provide advanced search capabilities executed on the server.

.. _SQLCatalog: https://intake-sql.readthedocs.io/en/latest/api.html#intake_sql.SQLCatalog


YAML Format
-----------

Intake catalogs can most simply be described with YAML files. This is very common
in the tutorials and this documentation, because it simple to understand, but demonstrate
the many features of Intake. Note that YAML files are also the easiest way to share
a catalog, simply by copying to a publicly-available location such as a cloud storage
bucket.
Here is an example:

.. code-block:: yaml

    metadata:
      version: 1
      parameters:
        file_name:
          type: str
          description: default file name for child entries
          default: example_file_name
    sources:
      example:
        description: test
        driver: random
        args: {}

      entry1_full:
        description: entry1 full
        metadata:
          foo: 'bar'
          bar: [1, 2, 3]
        driver: csv
        args: # passed to the open() method
          urlpath: '{{ CATALOG_DIR }}/entry1_*.csv'

      entry1_part:
        description: entry1 part
        parameters: # User parameters
          part:
            description: section of the data
            type: str
            default: "stable"
            allowed: ["latest", "stable"]
        driver: csv
        args:
          urlpath: '{{ CATALOG_DIR }}/entry1_{{ part }}.csv'

      entry2:
        description: entry2
        driver: csv
        args:
          # file_name parameter will be inherited from file-level parameters, so will
          # default to "example_file_name"
          urlpath: '{{ CATALOG_DIR }}/entry2/{{ file_name }}.csv`


Metadata
''''''''

Arbitrary extra descriptive information can go into the metadata section. Some fields will be
claimed for internal use and some fields may be restricted to local reading; but for now the only
field that is expected is ``version``, which will be updated when a breaking change is made to the
file format. Any catalog will have ``.metadata`` and ``.version`` attributes available.

Note that each source also has its own metadata.

The metadata section an also contain ``parameters`` which will be inherited by the sources in
the file (note that these sources can augment these parameters, or override them with their own
parameters).

Extra drivers
'''''''''''''

The ``driver:`` entry of a data source specification can be a driver name, as has been shown in the examples so far.
It can also be an absolute class path to use for the data source, in which case there will be no ambiguity about how
to load the data. That is the the preferred way to be explicit, when the driver name alone is not enough
(see `Driver Selection`_, below).

.. code-block:: yaml

    plugins:
      source:
        - module: intake.catalog.tests.example1_source
    sources:
      ...

However, you do not, in general, need to do this, since the ``driver:`` field of
each source can also explicitly refer to the plugin class.

Sources
'''''''

The majority of a catalog file is composed of data sources, which are named data sets that can be loaded for the user.
Catalog authors describe the contents of data set, how to load it, and optionally offer some customization of the
returned data.  Each data source has several attributes:

- ``name``: The canonical name of the source.  Best practice is to compose source names from valid Python identifiers.
  This allows Intake to support things like tab completion of data source names on catalog objects.
  For example, ``monthly_downloads`` is a good source
  name.
- ``description``: Human readable description of the source.  To help catalog browsing tools, the description should be
  Markdown.

- ``driver``: Name of the Intake :term:`Driver` to use with this source.  Must either already be installed in the current
  Python environment (i.e. with conda or pip) or loaded in the ``plugin`` section of the file. Can be a simple
  driver name like "csv" or the full path to the implementation class like "package.module.Class".

- ``args``: Keyword arguments to the init method of the driver.  Arguments may use template expansion.

- ``metadata``: Any metadata keys that should be attached to the data source when opened.  These will be supplemented
  by additional metadata provided by the driver.  Catalog authors can use whatever key names they would like, with the
  exception that keys starting with a leading underscore are reserved for future internal use by Intake.

- ``direct_access``: Control whether the data is directly accessed by the client, or proxied through a catalog server.
  See :ref:`remote-catalogs` for more details.

- ``parameters``: A dictionary of data source parameters.  See below for more details.

Caching Source Files Locally
''''''''''''''''''''''''''''

*This method of defining the cache  with a dedicated block is deprecated, see the Remote Access
section, below*

To enable caching on the first read of remote data source files, add the ``cache`` section with the
following attributes:

- ``argkey``: The args section key which contains the URL(s) of the data to be cached.
- ``type``: One of the keys in the cache registry [`intake.source.cache.registry`], referring to an implementation of caching behaviour. The default is "file" for the caching of one or more files.

Example:

.. code-block:: yaml

  test_cache:
    description: cache a csv file from the local filesystem
    driver: csv
    cache:
      - argkey: urlpath
        type: file
    args:
      urlpath: '{{ CATALOG_DIR }}/cache_data/states.csv'

The ``cache_dir`` defaults to ``~/.intake/cache``, and can be specified in the intake configuration
file or ``INTAKE_CACHE_DIR``
environment variable, or at runtime using the ``"cache_dir"`` key of the configuration.
The special value ``"catdir"`` implies that cached files will appear in the same directory as the
catalog file in which the data source is defined, within a directory named "intake_cache". These will
not appear in the cache usage reported by the CLI.

Optionally, the cache section can have a ``regex`` attribute, that modifies the path of the cache on
the disk. By default, the cache path is made by concatenating ``cache_dir``, dataset name, hash of
the url, and the url itself (without the protocol). ``regex`` attribute allows to remove part of the
url (the matching part).

Caching can be disabled at runtime for all sources regardless of the catalog specification::

    from intake.config import conf

    conf['cache_disabled'] = True

By default, progress bars are shown during downloads if the package ``tqdm`` is
available, but this can be disabled (e.g., for
consoles that don't support complex text) with

    conf['cache_download_progress'] = False

or, equivalently, the environment parameter ``INTAKE_CACHE_PROGRESS``.


The "types" of caching are that supported are listed in ``intake.source.cache.registry``, see
the docstrings of each for specific parameters that should appear in the cache block.


It is possible to work with compressed source files by setting ``type: compression`` in the cache specification.
By default the compression type is inferred from the file extension, otherwise it can be set by assigning the ``decomp``
variable to any of the options listed in ``intake.source.decompress.decomp``.
This will extract all the file(s) in the compressed file referenced by urlpath and store them in the cache directory.

In cases where miscellaneous files are present in the compressed file, a ``regex_filter`` parameter can be used. Only
the extracted filenames that match the pattern will be loaded. The cache path is appended to the filename so it is
necessary to include a wildcard to the beginning of the pattern.

Example:

.. code-block:: yaml

  test_compressed:
    driver: csv
    args:
      urlpath: 'compressed_file.tar.gz'
    cache:
      - type: compressed
        decomp: tgz
        argkey: urlpath
        regex_filter: '.*data.csv'

Templating
----------

Intake catalog files support Jinja2 templating for driver arguments. Any occurrence of
a substring like ``{{field}}`` will be replaced by the value of the user parameters with
that same name, or the value explicitly provided by the user. For how to specify these user parameters,
see the next section.

Some additional values are available for templating. The following is always available:
``CATALOG_DIR``, the full path to the directory containing the YAML catalog file.  This is especially useful
for constructing paths relative to the catalog directory to locate data files and custom drivers.
For example, the search for CSV files for the two "entry1" blocks, above, will happen in the same directory as
where the catalog file was found.

The following functions `may` be available. Since these execute code, the user of a catalog may decide
whether they trust those functions or not.

- ``env("USER")``: look in the set environment variables for the named variable
- ``client_env("USER")``: exactly the same, except that when using a client-server topology, the
  value will come from the environment of the client.
- ``shell("get_login thisuser -t")``: execute the command, and use the output as the value. The
  output will be trimmed of any trailing whitespace.
- ``client_shell("get_login thisuser -t")``: exactly the same, except that when using a client-server
  topology, the value will come from the system of the client.

The reason for the "client" versions of the functions is to prevent leakage of potentially sensitive
information between client and server by controlling where lookups happen. When working without a server,
only the ones without "client" are used.

An example:

.. code-block:: yaml

    sources:
      personal_source:
        description: This source needs your username
        args:
          url: "http://server:port/user/{{env(USER)}}"

Here, if the user is named "blogs", the ``url`` argument will resolve to
``"http://server:port/user/blogs"``; if the environment variable is not defined, it will
resolve to ``"http://server:port/user/"``

.. _paramdefs:

Parameter Definition
--------------------

A source definition can contain a "parameters" block.
Expressed in YAML, a parameter may look as follows:

.. code-block:: yaml

    parameters:
      name:
        description: name to use  # human-readable text for what this parameter means
        type: str  # one of bool, str, int, float, list[str], list[int], list[float], datetime
        default: normal  # optional, value to assume if user does not override
        allowed: ["normal", "strange"]  # optional, list of values that are OK, for validation
        min: "n"  # optional, minimum allowed, for validation
        max: "t"  # optional, maximum allowed, for validation

A parameter, not to be confused with an :term:`argument`,
can have one of two uses:

- to provide values for variables to be used in templating the arguments. *If* the pattern "{{name}}" exists in
  any of the source arguments, it will be replaced by the value of the parameter. If the user provides
  a value (e.g., ``source = cat.entry(name='something")``), that will be used, otherwise the default value. If
  there is no user input or default, the empty value appropriate for type is used. The ``default`` field allows
  for the same function expansion as listed for arguments, above.

- *If* an argument with the same name as the parameter exists, its value, after any templating, will be
  coerced to the given type of the parameter and validated against the allowed/max/min. It is therefore possible
  to use the string templating system (e.g., to get a value from the environment), but pass the final value as,
  for example, an integer. It makes no sense to provide a default for this case (the argument already has a value),
  but providing a default will not raise an exception.

Note: the ``datetime`` type accepts multiple values:
Python datetime, ISO8601 string,  Unix timestamp int, "now" and  "today".

Driver Selection
----------------

In some cases, it may be possible that multiple backends are capable of loading from the same data
format or service. Sometimes, this may mean two drivers with unique names, or a single driver
with a parameter to choose between the different backends.

However, it is possible that multiple drivers for reading a particular type of data
also share the same driver name: for example, both the
intake-iris and the intake-xarray packages contain drivers with the name ``"netcdf"``, which
are capable of reading the same files, but with different backends. Here we will describe the
various possibilities of coping with this situation. Intake's plugin system makes it easy to encode such choices.

It may be
acceptable to use any driver which claims to handle that data type, or to give the option of
which driver to use to the user, or it may be necessary to specify which precise driver(s) are
appropriate for that particular data. Intake allows all of these possibilities, even if the
backend drivers require extra arguments.

Specifying a single driver explicitly, rather than using a generic name, would look like this:

.. code-block:: yaml

    sources:
      example:
        description: test
        driver: package.module.PluginClass
        args: {}

It is also possible to describe a list of drivers with the same syntax. The first one
found will be the one used. Note that the class imports will only happen at data source
instantiation, i.e., when the entry is selected from the catalog.

.. code-block:: yaml

    sources:
      example:
        description: test
        driver:
          - package.module.PluginClass
          - another_package.PluginClass2
        args: {}

These alternative plugins can also be given data-source specific names, allowing the
user to choose at load time with `driver=` as a parameter. Additional arguments may also
be required for each option (which, as usual, may include user parameters); however, the
same global arguments will be passed to all of the drivers listed.


.. code-block:: yaml

    sources:
      example:
        description: test
        driver:
          first:
            class: package.module.PluginClass
            args:
              specific_thing: 9
          second:
            class: another_package.PluginClass2
        args: {}

Remote Access
-------------

(see also :ref:`remote_data` for the implementation details)

Many drivers support reading directly from remote data sources such as HTTP, S3 or GCS. In these cases,
the path to read from is usually given with a protocol prefix such as ``gcs://``. Additional dependencies
will typically be required (``requests``, ``s3fs``, ``gcsfs``, etc.), any data package
should specify these.  Further parameters
may be necessary for communicating with the storage backend and, by convention, the driver should take
a parameter ``storage_options`` containing arguments to pass to the backend. Some
remote backends may also make use of environment variables or config files to
determine thier default behaviour.

The special template variable "CATALOG_DIR" may be used to construct relative URLs in the arguments to
a source. In such cases, if the filesystem used to load that catalog contained arguments, then
the ``storage_options`` of that file system will be extracted and passed to the source. Therefore, all
sources which can accept general URLs (beyond just local paths) must make sure to accept this
argument.

As an example of using ``storage_options``, the following
two sources would allow for reading CSV data from S3 and GCS backends without
authentication (anonymous access), respectively

.. code-block:: yaml

   sources:
     s3_csv:
       driver: csv
       description: "Publicly accessible CSV data on S3; requires s3fs"
       args:
         urlpath: s3://bucket/path/*.csv
         storage_options:
           anon: true
     gcs_csv:
       driver: csv
       description: "Publicly accessible CSV data on GCS; requires gcsfs"
       args:
         urlpath: gcs://bucket/path/*.csv
         storage_options:
           token: "anon"

.. _caching:

Caching
'''''''

URLs interpreted by ``fsspec`` offer `automatic caching`_. For example, to enable
file-based caching for the first source above, you can do:

.. code-block:: yaml

   sources:
     s3_csv:
       driver: csv
       description: "Publicly accessible CSV data on S3; requires s3fs"
       args:
         urlpath: simplecache::s3://bucket/path/*.csv
         storage_options:
           s3:
             anon: true

Here we have added the "simplecache" to the URL (this caching backend does not store any
metadata about the cached file) and specified that the "anon" parameter is
meant as an argument to s3, not to the caching mechanism. As each file in
s3 is accessed, it will first be downloaded and then the local version
used instead.

.. _automatic caching: https://filesystem-spec.readthedocs.io/en/latest/features.html#caching-files-locally

You can tailor how the caching works. In particular the location of the local
storage can be set with the ``cache_storage`` parameter (under the "simplecache"
group of storage_options, of course) - otherwise they are stored in a temporary
location only for the duration of the current python session. The cache location
is particularly useful in conjunction with an environment variable, or
relative to "{{CATALOG_DIR}}", wherever the catalog was loaded from.

Please see the ``fsspec`` documentation for the full set of cache types and their
various options.

Local Catalogs
--------------

A Catalog can be loaded from a YAML file on the local filesystem by creating a Catalog object:

.. code-block:: python

    from intake import open_catalog
    cat = open_catalog('catalog.yaml')

Then sources can be listed:

.. code-block:: python

    list(cat)

and data sources are loaded via their name:

.. code-block:: python

    data = cat.entry_part1

and you can optionally configure new instances of the source to define user parameters
or override arguments by calling either of:

.. code-block:: python

    data = cat.entry_part1.configure_new(part='1')
    data = cat.entry_part1(part='1')  # this is a convenience shorthand

Intake also supports loading a catalog from all of the files ending in ``.yml`` and ``.yaml`` in a directory, or by using an
explicit glob-string. Note that the URL provided may refer to a remote storage systems by passing a protocol
specifier such as ``s3://``, ``gcs://``.:

.. code-block:: python

    cat = open_catalog('/research/my_project/catalog.d/')

Intake Catalog objects will automatically reload changes or new additions to catalog files and directories on disk.
These changes will not affect already-opened data sources.


Catalog Nesting
---------------

A catalog is just another type of data source for Intake. For example, you can print a YAML
specification corresponding to a catalog as follows:

.. code-block:: python

    cat = intake.open_catalog('cat.yaml')
    print(cat.yaml())

results in:

.. code-block:: yaml

    sources:
      cat:
        args:
          path: cat.yaml
        description: ''
        driver: intake.catalog.local.YAMLFileCatalog
        metadata: {}

The `point` here, is that this can be included in another catalog.
(It would, of course, be better to include a description and the full path of the catalog
file here.)
If the entry above were saved to another file, "root.yaml", and the
original catalog contained an entry, ``data``, you could access it as:

.. code-block:: python

    root = intake.open_catalog('root.yaml')
    root.cat.data



It is, therefore, possible to build up a hierarchy of catalogs referencing each other.
These can, of course, include remote URLs and indeed catalog sources other than simple files (all the
tables on a SQL server, for instance). Plus, since the argument and parameter system also
applies to entries such as the example above, it would be possible to give the user a runtime
choice of multiple catalogs to pick between, or have this decision depend on an environment
variable.

.. _remote-catalogs:

Server Catalogs
---------------

Intake also includes a server which can share an Intake catalog over HTTP
(or HTTPS with the help of a TLS-enabled reverse proxy).  From the user perspective, remote catalogs function
identically to local catalogs:

.. code-block:: python

    cat = open_catalog('intake://catalog1:5000')
    list(cat)

The difference is that operations on the catalog translate to requests sent to the catalog server.  Catalog servers
provide access to data sources in one of two modes:

* Direct access: In this mode, the catalog server tells the client how to load the data, but the client uses its
  local drivers to make the connection.  This requires the client has the required driver already installed *and*
  has direct access to the files or data servers that the driver will connect to.

* Proxied access: In this mode, the catalog server uses its local drivers to open the data source and stream the data
  over the network to the client.  The client does not need *any* special drivers to read the data, and can read data
  from files and data servers that it cannot access, as long as the catalog server has the required access.

Whether a particular catalog entry supports direct or proxied access is determined by the ``direct_access`` option:


- ``forbid`` (default): Force all clients to proxy data through the catalog server

- ``allow``: If the client has the required driver, access the source directly, otherwise proxy the data through the
  catalog server.

- ``force``: Force all clients to access the data directly.  If they do not have the required driver, an exception will
  be raised.

Note that when the client is loading a data source via direct access, the catalog server will need to send the driver
arguments to the client.  Do not include sensitive credentials in a data source that allows direct access.

Client Authorization Plugins
''''''''''''''''''''''''''''

Intake servers can check if clients are authorized to access the catalog as a whole, or individual catalog entries.
Typically a matched pair of server-side plugin (called an "auth plugin") and a client-side plugin (called a "client
auth plugin) need to be enabled for authorization checks to work.  This feature is still in early development, but see
module ``intake.auth.secret`` for a demonstration pair of server and client classes implementation auth via a shared
secret. See :doc:`auth-plugins`.
