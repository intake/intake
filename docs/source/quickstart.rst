Quickstart
==========

This guide will show you how to get started using Intake to read data.  It assumes you are working in either a conda or a virtualenv/pip environment.

Installation
------------

If you are using Anaconda or Miniconda, install Intake and a sample plugin with the following commands::

    conda install -c intake intake

If you are using virtualenv/pip, run the following commands (**does not work until Intake is on PyPI**)::

    pip install intake

Creating Sample Data
--------------------

Let's begin by creating a sample data set and catalog.  At the command line, run the ``intake example`` command.
This will create an example data catalog and two CSV data files.  These files contains some basic facts about the 50
US states.

Loading a Data Source
---------------------

Data sources can be created directly with the ``open_*()`` methods in the ``intake`` module.  To read our example data::

    >>> import intake
    >>> ds = intake.open_csv('states_*.csv')
    >>> print(ds)
    <intake.source.csv.CSVSource object at 0x1163882e8>

Each open method has different arguments, specific for the data format or service being used.

Reading Data
------------

Intake reads data into memory using containers you are already familiar with:

  * Tables: Pandas DataFrames
  * Multidimensional arrays: NumPy arrays
  * Semistructred data: Python lists of objects (usually dictionaries)

To find out what kind of container a data source will produce, inspect the ``container`` attribute::

    >>> ds.container
    'dataframe'

The result will be ``dataframe``, ``ndarray``, or ``python``.  (New container types will be added in the future.)

For data that fits in memory, you can ask Intake to load it directly::

    >>> df = ds.read()
    >>> df.head()
            state        slug code                nickname  ...
    0     Alabama     alabama   AL      Yellowhammer State
    1      Alaska      alaska   AK       The Last Frontier
    2     Arizona     arizona   AZ  The Grand Canyon State
    3    Arkansas    arkansas   AR       The Natural State
    4  California  california   CA            Golden State

Many data sources will also have quick-look plotting available. The attribute ``.plot`` will list a number of built-in
plotting methods, such as ``.scatter()``.

Intake data sources can have *partitions*.  A partition refers to a contiguous chunk of data that can be loaded independent of any other partition.  The partitioning scheme is entirely up to the plugin author.  In the case of the CSV plugin, each ``.csv`` file is a partition.

To read data from a data source one chunk at a time, the ``read_chunked()`` method returns an iterator::

    >>> for chunk in ds.read_chunked(): print('Chunk: %d' % len(chunk))
    ...
    Chunk: 24
    Chunk: 26


Working with Dask
-----------------

Working with large datasets is much easier with a parallel, out-of-core computing library like `Dask <https://dask.pydata.org/en/latest/>`_.  Intake can create Dask containers (like ``dask.dataframe``) from data sources that will load their data only when required::

    >>> ddf = ds.to_dask()
    >>> ddf
    Dask DataFrame Structure:
                admission_date admission_number capital_city capital_url    code constitution_url facebook_url landscape_background_url map_image_url nickname population population_rank skyline_background_url    slug   state state_flag_url state_seal_url twitter_url website
    npartitions=2
                        object            int64       object      object  object           object       object                   object        object   object      int64           int64                 object  object  object         object         object      object  object
                            ...              ...          ...         ...     ...              ...          ...                      ...           ...      ...        ...             ...                    ...     ...     ...            ...            ...         ...     ...
                            ...              ...          ...         ...     ...              ...          ...                      ...           ...      ...        ...             ...                    ...     ...     ...            ...            ...         ...     ...
    Dask Name: from-delayed, 4 tasks

The Dask containers will be partitioned in the same way as the Intake data source, allowing different chunks to be processed in parallel.

Opening a Catalog
-----------------

It is often useful to move the descriptions of data sources out of your code and into a configuration file that can be reused and shared with other projects and people.  Intake calls this a "catalog", which contains a list of named entries describing how to load data sources.  The ``intake example`` created a catalog file with the following contents:

.. code-block:: yaml

    sources:
      states
        description: US state information from [CivilServices](https://civil.services/)
        driver: csv
        args:
          urlpath: '{{ CATALOG_DIR }}/states_*.csv'
        metadata:
          origin_url: 'https://github.com/CivilServiceUSA/us-states/blob/v1.0.0/data/states.csv'

To load a catalog from a catalog file::

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
are appropriate for the particular data source.


Installing Data Source Packages with Conda
------------------------------------------

Intake makes it possible to create conda packages that install data sources into a global catalog.  For example, we can
install a data package containing the same data we have been working with::

    conda install -c intake data-us-states

Conda installs the catalog file in this package to ``$CONDA_PREFIX/share/intake/us_states.yml``.  Now, when we import
``intake``, we will see the data from this package appear as part of a global catalog called ``intake.cat``::

    >>> import intake
    >>> intake.cat.states.to_dask()[['state','slug']].head()
            state        slug
    0     Alabama     alabama
    1      Alaska      alaska
    2     Arizona     arizona
    3    Arkansas    arkansas
    4  California  california

The global catalog is a union of all catalogs installed in the conda/virtualenv environment and also any catalogs
installed in user-specific location.

Using the GUI
-------------

A graphical data browser is available in the Jupyter notebook environment. It will show the
contents of any installed catalogs, plus allows for selecting local and remote catalogs,
to browse and select entries from these. See :ref:`gui`