Catalogs
========

Data catalogs provide an abstraction that allows you to externally define, and optionally share, descriptions of
datasets, called *catalog entries*.  A catalog entry for a dataset includes information like:

* The name of the Intake driver that can load the data
* Arguments to the ``__init__()`` method of the driver
* Metadata provided by the catalog author (such as field descriptions and types, or data provenance)

In addition, Intake allows datasets to be *parameterized* in the catalog.  This is most commonly used to allow the
user to filter down datasets at load time, rather than having to bring everything into memory first.  The data
parameters are defined by the catalog author, then templated into the arguments for ``__init__()`` to modify the data
being loaded.  This approach is less flexible for the end user than something like the
`Blaze expression system <https://blaze.readthedocs.io/en/latest/expr-compute-dev.html>`_, but also significantly
reduces the implementation burden for driver authors.

YAML Format
-----------

Intake catalogs are typically described with YAML files.  Here is an example:

.. code-block:: yaml

    metadata:
      version: 1
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
        parameters: # User defined parameters
          part:
            description: part of filename
            type: str
            default: "1"
            allowed: ["1", "2"]
        driver: csv
        args:
          urlpath: '{{ CATALOG_DIR }}/entry1_{{ part }}.csv'

Templating
''''''''''

Intake catalog files support Jinja2 templating for driver arguments. Any occurrence of
a substring like ``{{field}}`` will be replaced by the value of the user parameters with
that same name. Some additional values are always available:

- ``CATALOG_DIR``: The full path to the directory containing the YAML catalog file.  This is especially useful
  for constructing paths relative to the catalog directory to locate data files and custom drivers.

Metadata
''''''''

Arbitrary extra descriptive information can go into the metadata section. Some fields will be
claimed for internal use and some fields may be restricted to local reading; but for now the only
field that is expected is ``version``, which will be updated when a breaking change is made to the
file format. Any catalog will have ``.metadata`` and ``.version`` attributes available.

Extra drivers
'''''''''''''

The ``driver:`` entry of a data source specification can be a driver name, as has been shown in the examples so far.
It can also be an absolute class path to use for the data source, in which case there will be no ambiguity about how
to load the data. That is the the preferred way to be explicit, when the driver name alone is not enough
(see `Driver Selection`_, below). However,
it is also possible to specify extra modules and directories to scan for plugins, as an alternative method for
finding driver classes.

In addition to using drivers already installed in the Python environment with conda or pip
(see :ref:`driver-discovery`), a catalog can also use additional drivers from arbitrary locations listed in the YAML
file:

.. code-block:: yaml

    plugins:
      source:
        - module: intake.catalog.tests.example1_source
        - dir: '{{ CATALOG_DIR }}/example_plugin_dir'
    sources:
      ...


The following import methods are allow:

- ``- module: my.module.path``: The Python module to import and search for driver classes.  This uses the standard
  notation of the Python ``import`` command and will search the PYTHONPATH in the same way.
- ``- dir: /my/module/directory``: All of the ``*.py`` files in this directory will be executed, and any driver
  classes found will be added to the catalog's plugin registry.  It is common for the directory of Python files to be
  stored relative to the catalog file itself, so using the ``CATALOG_DIR`` variable will allow that relative path to be
  specified.

Each of the above methods can be used multiple times, and in combination, to load as many extra drivers as are needed.
Most drivers should be installed as Python packages (enabling autodiscovery), but sometimes catalog-specific drivers may
be needed to perform specific data transformations that are not broadly applicable enough to warrant creating a
dedicated package.  In those cases, the above options allow the drivers to be bundled with the catalog instead.


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
  Python environment (i.e. with conda or pip) or loaded in the ``plugin`` section of the file.

- ``args``: Keyword arguments to the ``open()`` method of the driver.  Arguments may use template expansion.

- ``metadata``: Any metadata keys that should be attached to the data source when opened.  These will be supplemented
  by additional metadata provided by the driver.  Catalog authors can use whatever key names they would like, with the
  exception that keys starting with a leading underscore are reserved for future internal use by Intake.

- ``direct_access``: Control whether the data is directly accessed by the client, or proxied through a catalog server.
  See :ref:`remote-catalogs` for more details.

- ``parameters``: A dictionary of data source parameters.  See below for more details.

Parameters allow the user to customize the data returned by a data source.  Most often, parameters are used to filter
or reduce the data in specific ways defined by the catalog author.  The parameters defined for a given data source are
available for use in template strings, which can be used to alter the arguments provided to the driver.  For example,
a data source might accept a "postal_code" argument which is used to alter a database query, or select a particular
group within a file.  Users set parameters with keyword arguments to the ``get()`` method on the catalog object.

Driver Selection
''''''''''''''''

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
instantiation.

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


Parameter Definition
^^^^^^^^^^^^^^^^^^^^

To enable users to discover parameters on data sources, and to allow UIs to generate interfaces automatically,
parameters have the following attributes in the catalog.

- ``description``: Human-readable Markdown description of what the parameter means.
- ``type``: The type of the parameter.  Currently, this may be ``bool``, ``str``, ``int``, ``float``, ``list[str]``,
  ``list[int]``, ``list[float]``, ``datetime``.

- ``default``: The default value for this parameter.  Every parameter must have a default to ensure a catalog user can
  quickly see some sample data.

- ``allowed`` (optional): A list of allowed values for this parameter
- ``min`` (optional): Minimum value (inclusive) for the parameter
- ``max`` (optional): Maximum value (inclusive) for the parameter

Note both ``allowed`` and ``min``/``max`` should not be set for the same parameter.

Also the ``datetime`` type accepts multiple values:

* a Python datetime object
* an ISO8601 timestamp string
* an integer representing a Unix timestamp
* ``now``, a string representing the current timestamp
* ``today``, a string representing today at midnight UTC

The ``default`` field allows for special syntax to get information from the system. This is
particularly useful for user credentials, which may be defined by environment variables or
fetched by running some external command. The special syntax are:

- ``env(USER)``: look in the environment for the named variable; in the example, this will
  be the username.
- ``client_env(USER)``: exactly the same, except that when using a client-server topology, the
  value will come from the environment of the client.
- ``shell(get_login thisuser -t)``: execute the command, and use the output as the value. The
  output will be trimmed of any trailing whitespace.
- ``client_shell(get_login thisuser -t)``: exactly the same, except that when using a client-server
  topology, the value will come from the system of the client.

Since it may not be desirable to have the access of
a catalog get information from the system, the keywords ``getenv`` and ``getshell`` (passed to
``Catalog``) allow these
mechanisms to by turned off, in which case the value of the default will still appear as the
original template string (and so the user should override with a value they have obtained
elsewhere). Note that in the case of a remote catalog, the client cannot see the values that
will be evaluated on the server side, the evaluation only happens if the user did not override
the value when accessing the data.

Caching Source Files Locally
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To enable caching on the first read of remote data source files, ``cache`` specifications have the following attributes
in the catalog.

- ``argkey``: Of the keys in the args section in this same data source, which contains the URL(s) of the data to be cached.
- ``regex``: A regular expression to match against the URL path, where the matching portion will be replaced by a path in the local cache directory.
- ``type``: One of the keys in the cache registry [`intake.source.cache.registry`], referring to an implementation of caching behaviour. The default if "file" for the caching of one or more specific remote files.

Example:

.. code-block:: yaml

  test_cache:
    description: cache a csv file from the local filesystem
    driver: csv
    cache:
      - argkey: urlpath
        regex: '{{ CATALOG_DIR }}/cache_data'
        type: file
    args:
      urlpath: '{{ CATALOG_DIR }}/cache_data/states.csv'

The ``cache_dir`` defaults to ``~/.intake/cache``, and can be specified in the intake configuration
file or ``INTAKE_CACHE_DIR``
environment variable, or at runtime using the ``"cache_dir"`` key of the configuration.
The special value ``"catdir"`` implies that cached files will appear in the same directory as the
catalog file in which the data source is defined, within a directory named "intake_cache". These will
not appear in the cache usage reported by the CLI.

Caching can be disabled at runtime for all sources regardless of the catalog specificiation::

    from intake.config import conf

    conf['cache_disabled'] = True

By default, progress bars are shown during downloads if the package ``tqdm`` is
available, but this can be disabled (e.g., for
consoles that don't support complex text) with

    conf['cache_download_progress'] = False

or, equivalently, the environment parameter ``INTAKE_CACHE_PROGRESS``.


The "types" of caching are that supported are listed in ``intake.source.cache.registry``, see
the docstrings of each for specific parameters that should appear in the cache block.


Local Catalogs
--------------

A Catalog can be loaded from a YAML file on the local filesystem by creating a Catalog object::

    from intake import load_catalog

    cat = load_catalog('catalog.yaml')

Then sources can be listed::

    list(cat)

and data sources are loaded via their name:

    data = cat.entry_part1(part='1')

Intake also supports loading all of the files ending in ``.yml`` and ``.yaml`` in a directory, or by using an
explicit glob-string. Note that the URL provided may refer to a remote storage systems by passing a protocol
specifier such as ``s3://``, ``gcs://``.::

    cat = load_catalog('/research/my_project/catalog.d/')

Intake Catalog objects will automatically detect changes or new additions to catalog files and directories on disk.
These changes will not affect already-opened data sources.

.. _remote-catalogs:

Remote Catalogs
---------------

Intake also includes a server which can share an Intake catalog over HTTP
(or HTTPS with the help of a TLS-enabled reverse proxy).  From the user perspective, remote catalogs function
identically to local catalogs::

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
