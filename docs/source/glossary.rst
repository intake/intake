Glossary
========

.. glossary::

    Argument
        One of a set of values passed to a function or class. In the Intake sense, this usually is the
        set of key-value pairs defined in the "args" section of a source definition; unless the user
        overrides, these will be used for instantiating the source.

    Cache
        Local copies of remote files. Intake allows for download-on-first-use for data-sources,
        so that subsequent access is much faster, see :ref:`caching`. The
        format of the files is unchanged in this case, but may be decompressed.

    Catalog
        A collection of entries, each of which corresponds to a specific :term:`Data-set`. Within these docs, a catalog is
        most commonly defined in a :term:`YAML` file, for simplicity, but there are other possibilities, such as connecting to an Intake
        server or another third-party data service, like a SQL database. Thus, catalogs form a hierarchy: any
        catalog can contain other, nested catalogs.

    Catalog file
        A :term:`YAML` specification file which contains a list of named entries describing how to load data
        sources. :doc:`catalog`.

    Conda
        A package and environment management package for the python ecosystem, see the `conda website`_. Conda ensures
        dependencies and correct versions are installed for you, provides precompiled, binary-compatible software,
        and extends to many languages beyond python, such as R, javascript and C.

    Conda package
        A single installable item which the :term:`Conda` application can install. A package may include
        a :term:`Catalog`, data-files and maybe some additional code. It will also include a specification of the
        dependencies that it requires (e.g., Intake and any additional :term:`Driver`), so that Conda can install those
        automatically. Packages can be created locally, or can be found on `anaconda.org`_ or other package
        repositories.

    Container
        One of the supported data formats. Each :term:`Driver` outputs its data in one of these. The
        containers correspond to familiar data structures for end-analysis, such as list-of-dicts, Numpy nd-array or
        Pandas data-frame.

    Data-set
        A specific collection of data. The type of data (tabular, multi-dimensional or something else) and the format
        (file type, data service type) are all attributes of the data-set. In addition, in the context of Intake,
        data-sets are usually entries within a :term:`Catalog` with additional descriptive text and metadata and
        a specification of *how* to load the data.

    Data Source
        An Intake specification for a specific :term:`Data-set`. In most cases, the two terms are
        synonymous.

    Data User
        A person who uses data to produce models and other inferences/conclusions. This
        person generally uses standard python analysis packages like Numpy, Pandas, SKLearn and may produce
        graphical output. They will want to be able to find the right data for a given job, and for
        the data to be available in a standard format as quickly and
        easily as possible. In many organisations, the appropriate job title may be Data Scientist, but
        research scientists and BI/analysts also fit this description.

    Data packages
        Data packages are standard conda packages that install an Intake catalog file into the user’s conda
        environment ($CONDA_PREFIX/share/intake). A data package does not necessarily imply there are data files
        inside the package. A data package could describe remote data sources (such as files in S3) and take up
        very little space on disk.

    Data Provider
        A person whose main objective is to curate data sources, get them into appropriate
        formats, describe the contents, and disseminate the data to those that need to use them. Such a person
        may care about the specifics of the storage format and backing store, the right number of fields
        to keep and removing bad data. They may have a good idea of the best way to visualise any give
        data-set. In an organisation, this job may be known as Data Engineer, but it could as easily be
        done by a member of the IT team. These people are the most likely to author :term:`Catalogs<Catalog>`.

    Developer
        A person who writes or fixes code. In the context of Intake, a developer may make new format
        :term:`Drivers<Driver>`, create authentication systems or add functionality to Intake itself. They can
        take existing code for loading data in other projects, and use Intake to add extra functionality to it,
        for instance, remote data access, parallel processing, or file-name parsing.

    Driver
        The thing that does the work of reading the data for a catalog entry is known as a driver, often referred
        to using a simple name such as "csv". Intake
        has a plugin architecture, and new drivers can be created or installed, and specific catalogs/data-sets may
        require particular drivers for their contained data-sets. If installed as :term:`Conda` packages, then
        these requirements will be automatically installed for you. The driver's output will be a :term:`Container`,
        and often the code is a simpler layer over existing functionality in a third-party package.

    GUI
        A Graphical User Interface. Intake comes with a GUI for finding and selecting data-sets, see :doc:`gui`.

    IT
        The Information Technology team for an organisation. Such a team may have
        control of the computing infrastructure and security (sys-ops), and may well act as gate-keepers when
        exposing data for use by other colleagues. Commonly, IT has stronger policy enforcement requirements
        that other groups, for instance requiring all data-set copy actions to be logged centrally.

    Persist
        A process of making a local version of a data-source. One canonical format is used for each
        of the container types, optimised for quick and parallel access. This is particularly useful
        if the data takes a long time to acquire, perhaps because it is the result of a complex
        query on a remote service. The resultant output can be set to expire and be automatically
        refreshed, see :doc:`persisting`. Not to be confused with the :term:`cache`.

    Plugin
        Modular extra functionality for Intake, provided by a package that is installed separately. The most common type of
        plugin will be for a :term:`Driver` to load some particular data format; but other parts of Intake are
        pluggable, such as authentication mechanisms for the server.

    Server
        A remote source for Intake catalogs. The server will
        provide data source specifications (i.e., a remote :term:`Catalog`), and may also provide the raw data, in situations
        where the client is not able or not allowed to access it directly. As such, the server can act as a gatekeeper of
        the data for security and monitoring purposes. The implementation of the server in Intake is accessible as the
        ``intake-server`` command, and acts as a reference: other implementations can easily be created for
        specific circumstances.

    TTL
        Time-to-live, how long before the given entity is considered to have expired. Usually in seconds.

    User Parameter
        A data source definition can contain a "parameters" section, which can act as explicit decision indicators
        for the user, or as validation and type coersion for the definition's :term:`Argument` s. See
        :ref:`paramdefs`.

    YAML
        A text-based format for expressing data with a dictionary (key-value) and list structure, with a limited
        number of data-types, such as strings and numbers. YAML uses indentations to nest objects, making it easy
        to read and write for humans, compared to JSON. Intake's catalogs and config are usually expressed in YAML
        files.


.. _conda website: https://conda.io/docs/
.. _anaconda.org: http://anaconda.org
