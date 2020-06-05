Quickstart
==========

This guide will show you how to get started using Intake to read data, and give you a flavour
of how Intake feels to the :term:`Data User`.
It assumes you are working in either a conda or a virtualenv/pip environment. For notebooks with
executable code, see the :doc:`examples`. This walk-through can be run from a notebook or interactive
python session.

Installation
------------

If you are using `Anaconda`_ or Miniconda, install Intake with the following commands::

    conda install -c conda-forge intake

If you are using virtualenv/pip, run the following command::

    pip install intake

Note that this will install with the minimum of optional requirements. If you want a more complete
install, use `intake[complete]` instead.

.. _Anaconda: https://www.anaconda.com/download/

Creating Sample Data
--------------------

Let's begin by creating a sample data set and catalog.  At the command line, run the ``intake example`` command.
This will create an example data :term:`Catalog` and two CSV data files.  These files contains some basic facts about the 50
US states, and the catalog includes a specification of how to load them.

Loading a Data Source
---------------------

:term:`Data sources<Data-set>` can be created directly with the ``open_*()`` functions in the ``intake``
module.  To read our example data::

    >>> import intake
    >>> ds = intake.open_csv('states_*.csv')
    >>> print(ds)
    <intake.source.csv.CSVSource object at 0x1163882e8>

Each open function has different arguments, specific for the data format or service being used.

Reading Data
------------

Intake reads data into memory using :term:`containers<Container>` you are already familiar with:

  * Tables: Pandas DataFrames
  * Multidimensional arrays: NumPy arrays
  * Semistructured data: Python lists of objects (usually dictionaries)

To find out what kind of container a data source will produce, inspect the ``container`` attribute::

    >>> ds.container
    'dataframe'

The result will be ``dataframe``, ``ndarray``, or ``python``.  (New container types will be added in
the future.)

For data that fits in memory, you can ask Intake to load it directly::

    >>> df = ds.read()
    >>> df.head()
            state        slug code                nickname  ...
    0     Alabama     alabama   AL      Yellowhammer State
    1      Alaska      alaska   AK       The Last Frontier
    2     Arizona     arizona   AZ  The Grand Canyon State
    3    Arkansas    arkansas   AR       The Natural State
    4  California  california   CA            Golden State

Many data sources will also have quick-look plotting available. The attribute ``.plot`` will list
a number of built-in plotting methods, such as ``.scatter()``, see :doc:`plotting`.

Intake data sources can have *partitions*.  A partition refers to a contiguous chunk of data that can be loaded
independent of any other partition.  The partitioning scheme is entirely up to the plugin author.  In
the case of the CSV plugin, each ``.csv`` file is a partition.

To read data from a data source one chunk at a time, the ``read_chunked()`` method returns an iterator::

    >>> for chunk in ds.read_chunked(): print('Chunk: %d' % len(chunk))
    ...
    Chunk: 24
    Chunk: 26


Working with Dask
-----------------

Working with large datasets is much easier with a parallel, out-of-core computing library like
`Dask <https://dask.pydata.org/en/latest/>`_.  Intake can create Dask containers (like ``dask.dataframe``)
from data sources that will load their data only when required::

    >>> ddf = ds.to_dask()
    >>> ddf
    Dask DataFrame Structure:
                admission_date admission_number capital_city capital_url    code constitution_url facebook_url landscape_background_url map_image_url nickname population population_rank skyline_background_url    slug   state state_flag_url state_seal_url twitter_url website
    npartitions=2
                        object            int64       object      object  object           object       object                   object        object   object      int64           int64                 object  object  object         object         object      object  object
                            ...              ...          ...         ...     ...              ...          ...                      ...           ...      ...        ...             ...                    ...     ...     ...            ...            ...         ...     ...
                            ...              ...          ...         ...     ...              ...          ...                      ...           ...      ...        ...             ...                    ...     ...     ...            ...            ...         ...     ...
    Dask Name: from-delayed, 4 tasks

The Dask containers will be partitioned in the same way as the Intake data source, allowing different chunks
to be processed in parallel. Please read the Dask documentation to understand the differences when
working with Dask collections (Bag, Array or Data-frames).

Opening a Catalog
-----------------

A :term:`Catalog` is a collection of data sources, with the type and arguments prescribed for each, and
arbitrary metadata about each source.
In the simplest case, a catalog can be described by a file in YAML format, a
":term:`Catalog file`". In real usage, catalogues can be defined in a number of ways, such as remote
files, by
connecting to a third-party data service (e.g., SQL server) or through an Intake :term:`Server` protocol, which
can implement any number of ways to search and deliver data sources.

The ``intake example`` command, above, created a catalog file
with the following :term:`YAML`-syntax content:

.. code-block:: yaml

    sources:
      states
        description: US state information from [CivilServices](https://civil.services/)
        driver: csv
        args:
          urlpath: '{{ CATALOG_DIR }}/states_*.csv'
        metadata:
          origin_url: 'https://github.com/CivilServiceUSA/us-states/blob/v1.0.0/data/states.csv'

To load a :term:`Catalog` from a :term:`Catalog file`::

    >>> cat = intake.open_catalog('us_states.yml')
    >>> list(cat)
    ['states']

This catalog contains one data source, called ``states``.  It can be accessed by attribute::

    >>> cat.states.to_dask()[['state','slug']].head()
            state        slug
    0     Alabama     alabama
    1      Alaska      alaska
    2     Arizona     arizona
    3    Arkansas    arkansas
    4  California  california

Placing data source specifications into a catalog like this enables declaring data sets in a single canonical place,
and not having to use boilerplate code in each notebook/script that makes use of the data. The catalogs can also
reference one-another, be stored remotely, and include extra metadata such as a set of named quick-look plots that
are appropriate for the particular data source. Note that catalogs are **not** restricted
to being stored in YAML files, that just happens to be the simplest way to display them.

Many catalog entries will also contain "user_parameter" blocks, which are indications of options explicitly
allowed by the catalog author, or for validation or the values passed. The user can customise how a data
source is accessed by providing values for the user_parameters, overriding the arguments specified in
the entry, or passing extra keyword arguments to be passed to the driver. The keywords that should
be passed are limited to the user_parameters defined and the inputs expected by the specific
driver - such usage is expected only from those already familiar with the specifics of the given
format. In the following example, the user overrides the "csv_kwargs" keyword, which is described
in the documentation for :func:`CSVSource <intake.source.csv.CSVSource>` and gets passed down to the CSV reader::

    # pass extra kwargs understood by the csv driver
    >>> intake.cat.states(csv_kwargs={'header': None, 'skiprows': 1}).read().head()
               0           1   ...                                17
    0     Alabama     alabama  ...    https://twitter.com/alabamagov
    1      Alaska      alaska  ...        https://twitter.com/alaska


Note that, if you are *creating* such catalogs, you may well start by trying the ``open_csv`` command,
above, and then use ``print(ds.yaml())``. If you do this now, you will see that the output is very
similar to the catalog file we have provided.

Installing Data Source Packages
-------------------------------

Intake makes it possible to create :term:`Data packages` (``pip`` or ``conda``)
that install data sources into a
global catalog.  For example, we can
install a data package containing the same data we have been working with::

    conda install -c intake data-us-states

:term:`Conda` installs the catalog file in this package to ``$CONDA_PREFIX/share/intake/us_states.yml``.
Now, when we import
``intake``, we will see the data from this package appear as part of a global catalog called ``intake.cat``. In this
particular case we use Dask to do the reading (which can handle larger-than-memory data and parallel
processing), but ``read()`` would work also::

    >>> import intake
    >>> intake.cat.states.to_dask()[['state','slug']].head()
            state        slug
    0     Alabama     alabama
    1      Alaska      alaska
    2     Arizona     arizona
    3    Arkansas    arkansas
    4  California  california

The global catalog is a union of all catalogs installed in the conda/virtualenv environment and also any catalogs
installed in user-specific locations.


Adding Data Source Packages using the Intake path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Intake checks the Intake config file for ``catalog_path`` or the environment variable ``"INTAKE_PATH"`` for a colon
separated list of paths (semicolon on windows) to search for catalog files.
When you import ``intake`` we will see all entries from all of the catalogues referenced as part of a global catalog
called ``intake.cat``.


Using the GUI
-------------

A graphical data browser is available in the Jupyter notebook environment or standalone web-server.
It will show the
contents of any installed catalogs, plus allows for selecting local and remote catalogs,
to browse and select entries from these. See :doc:`gui`.
