Catalogs
========

Data catalogs provide an abstraction that allows you to externally define, and optionally share, descriptions of datasets, called *catalog entries*.  A catalog entry for a dataset includes information like:

* The name of the Intake plugin that can load the data
* Arguments to the ``open()`` method of the plugin
* Metadata provided by the catalog author (such as field descriptions and types, or data provenance)

In addition, Intake allows datasets to be *parameterized* in the catalog.  This is most commonly used to allow the user to filter down datasets at load time, rather than having to bring everything into memory first.  The data parameters are defined by the catalog author, then templated into the arguments for `open()` to modify the data being loaded.  This approach is less flexible for the end user than something like the `Blaze expression system <https://blaze.readthedocs.io/en/latest/expr-compute-dev.html>`_, but also significantly reduces the implementation burden for plugin authors.

YAML Format
-----------

Intake catalogs are described with YAML files.  Here is an example:

.. code-block:: yaml

    sources:
      - name: example
        description: test  
        driver: random
        args: {}

      - name: entry1_full
        description: entry1 full
        metadata:
          foo: 'bar'
          bar: [1, 2, 3]
        driver: csv
        args: # passed to the open() method
          urlpath: !template '{{ CATALOG_DIR }}/entry1_*.csv'

      - name: entry1_part
        description: entry1 part
        parameters: # User defined parameters
          part:
            description: part of filename
            type: str
            default: "1"
            allowed: ["1", "2"]
        driver: csv
        args:
          urlpath: !template '{{ CATALOG_DIR }}/entry1_{{ part }}.csv'

Templating
''''''''''

Intake catalog files support special string types which are actually Jinja2 templates.  These are indicated with the YAML type syntax:

- ``!template``: a template that yields a string
- (TBD) ``!template_int``: a template that is cast to an integer (using the Python `int()` function) after template execution
- (TBD) ``!template_datetime``: a template that is cast to a Python datetime object assuming it is an ISO 8601 timestamp

Typically, templates are used to construct plugin arguments based on user-provided parameters in the context of a particular catalog entry.  There are also special Jinja2 variables functions that are automatically available in all template strings:

- ``CATALOG_DIR``: The full path to the directory containing the YAML catalog file.  This is especially useful for constructing paths relative to the catalog directory to locate data files and custom plugins.
- (TBD) ``env(ENV_VAR)``: Access an environment variable (useful for passing tokens that should not be hardcoded into the file)
- (TBD) ``shell(CMD)``: Run a command in the shell each time the source is loaded from the catalog and return standard out.  If the command does not have a zero return code, then an exception will be raised, aborting the operation in progress.

.. warning::

    TBD: explain best practices to avoid file system and SQL injection attacks.


Extra Plugins
'''''''''''''

In addition to using plugins already installed in the Python environment with conda or pip (see :ref:`plugin-discovery`), a catalog can also use additional plugins from aribtrary locations listed in the YAML file:

.. code-block:: yaml

    plugins:
      source:
        - module: intake.catalog.tests.example1_source
        - dir: !template '{{ CATALOG_DIR }}/example_plugin_dir'
    sources:
      - ...


The following import methods are allow:

- ``- module: my.module.path``: The Python module to import and search for plugin classes.  This uses the standard notation of the Python ``import`` command and will search the PYTHONPATH in the same way.
- ``- dir: /my/module/directory``: All of the ``*.py`` files in this directory will be executed, and any plugin classes found will be added to the catalog's plugin registry.  It is common for the directory of Python files to be stored relative to the catalog file itself, so using a ``!template`` string with the ``CATALOG_DIR`` variable will allow that relative path to be specified.

Each of the above methods can be used multiple times, and in combination, to load as many extra plugins as are needed.  Most plugins should be installed as Python packages (enabling autodiscovery), but sometimes catalog-specific plugins may be needed to perform specific data transformations that are not broadly applicable enough to warrant creating a dedicated package.  In those cases, the above options allow the plugins to be bundled with the catalog instead.


Sources
'''''''

The majority of a catalog file is a list of data sources, which are named data sets that can be loaded for the user.  Catalog authors describe the cotents of data set, how to load it, and optionally offer some customization of the returned data.  Each data source has several attributes:

- ``name``: The canonical name of the source.  Best practice is to compose source names from valid Python identifiers separated by dots.  This allows Intake to support things like tab completion of data source names on catalog objects. For example, ``monthly_downloads``, ``ops.servers.cpu_status``, and ``region1.satellite.IR`` are all good source names.  Tools that display Intake catalogs should interpret the dot notation as describing a hierarchy.
- ``description``: Human readable description of the source.  To help catalog browsing tools, the description should be Markdown.
- ``driver``: Name of the Intake plugin to use with this source.  Must either already be installed in the current Python environment (i.e. with conda or pip) or loaded in the ``plugin`` section of the file.
- ``args``: Keyword arguments to the ``open()`` method of the plugin.  Arguments may use template expansion.
- ``metadata``: Any metadata keys that should be attached to the data source when opened.  These will be supplemented by additional metadata provided by the plugin.  Catalog authors can use whatever key names they would like, with the exception that keys starting with a leading underscore are reserved for future internal use by Intake.
- ``direct_access``: Control whether the data is directly accessed by the client, or proxied through a catalog server.  See :ref:`remote-catalogs` for more details.
- ``parameters``: A dictionary of data source parameters.  See below for more details.

Parameters allow the user to customize the data returned by a data source.  Most often, parameters are used to filter or reduce the data in specific ways defined by the catalog author.  The parameters defined for a given data source are available for use in template strings, which can be used to alter the arguments provided to the plugin.  For example, a data source might accept a "postal_code" argument which is used to alter a database query, or select a particular group within a file.  Users set parameters with keyword arguments to the ``get()`` method on the catalog object.

Parameter Definition
^^^^^^^^^^^^^^^^^^^^

To enable users to discover parameters on data sources, and to allow UIs to generate interfaces automatically, parameters have the following attributes in the catalog.

- ``description``: Human-readable Markdown description of what the parameter means.
- ``type``: The type of the parameter.  Currently, this may be ``bool``, ``str``, ``int``, ``float``, ``list[str]``, ``list[int]``, ``list[float]``, ``datetime`` (which accepts either a Python datatime object or an ISO8601 timestamp string).
- ``default``: The default value for this parameter.  Every parameter must have a default to ensure a catalog user can quickly see some sample data.
- ``allowed`` (optional): A list of allowed values for this parameter
- ``min`` (optional): Minimum value (inclusive) for the parameter
- ``max`` (optional): Maximum value (inclusive) for the parameter

Note both ``allowed`` and ``min``/``max`` should not be set for the same parameter.


Local Catalogs
--------------

A Catalog can be loaded from a YAML file on the local filesystem by creating a Catalog object::

    from intake import Catalog

    cat = Catalog('catalog.yaml')

Then sources can be listed::

    cat.list()

and data sources are loaded with ``get()``::

    data = cat.get('entry1_part', part='1')

Intake also supports loading all of the files ending in ``.yml`` and ``.yaml`` in a directory::

    cat = Catalog('/research/my_project/catalog.d/')

Intake Catalog objects will automatically detect changes or new additions to catalog files and directories on disk.  These changes will not affect already-opened data sources.

.. _remote-catalogs:

Remote Catalogs
---------------

Intake also includes a server which can share an Intake catalog over HTTP (or HTTPS with the help of a TLS-enabled reverse proxy).  From the user perspective, remote catalogs function identically to local catalogs::

    cat = Catalog('http://catalog1:5000')
    cat.list()

The difference is that operations on the catalog translate to requests sent to the catalog server.  Catalog servers provide access to data sources in one of two modes:

* Direct access: In this mode, the catalog server tells the client how to load the data, but the client uses its local plugins to make the connection.  This requires the client has the required plugin already installed *and* has direct access to the files or data servers that the plugin will connect to.

* Proxied access: In this mode, the catalog server uses its local plugins to open the data source and stream the data over the network to the client.  The client does not need *any* special plugins to read the data, and can read data from files and data servers that it cannot access, as long as the catalog server has the required access.

Whether a particular catalog entry supports direct or proxied access is determined by the ``direct_access`` option:

- ``forbid`` (default): Force all clients to proxy data through the catalog server
- ``allow``: If the client has the required plugin, access the source directly, otherwise proxy the data through the catalog server.
- ``force``: Force all clients to access the data directly.  If they do not have the required plugin, an exception will be raised.

Note that when the client is loading a data source via direct access, the catalog server will need to send the plugin arguments to the client.  Do not include sensitive credentials in a data source that allows direct access.
