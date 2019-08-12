Deployment Scenarios
--------------------

In the following sections, we will describe some of the ways in which Intake is used in real
production systems. These go well beyond the typical YAML files presented in the quickstart
and examples sections, which are necessarily short and simple, and do not demonstrate the
full power of Intake.

Sharing YAML files
~~~~~~~~~~~~~~~~~~

This is the simplest scenario, and amply described in these documents. The primary
advantage is simplicity: it is enough to put a file in an accessible place (even
a gist or repo), in order
for someone else to be able to discover and load that data. Furthermore, such
files can easily refer to one-another, to build up a full tree of data assets with
minimum pain Since YAML files are
text, this also lends itself to working well with version control systems.
Furthermore, all sources can describe themselves as YAML, and the
``export`` and ``upload`` commands can produce an efficient format (possibly remote) together
with YAML definition in a single step.

Pangeo
~~~~~~

The `Pangeo`_ collaboration uses Intake to catalog their data holdings, which are generally
in various forms of netCDF-compliant formats, massive multi-dimensional arrays with data
relating to earth and climate science and meteorology. On their cloud-based platform,
containers start up jupyter-lab sessions which have Intake installed, and therefore can
simply pick and load the data that each researcher needs - often requiring large Dask
clusters to actually do the processing.

A `static <https://pangeo-data.github.io/pangeo-datastore/>`__ rendering of the catalog
contents is available, so that users can browse the holdings
without even starting a python session. This rendering is produced by CI on the
`repo <https://github.com/pangeo-data/pangeo-datastore>`__ whenever new definitions are
added, and it also checks (using Intake) that each definition is indeed loadable.

Pangeo also developed intake-stac, which can talk to STAC servers to make real-time
queries and parse the results into Intake data sources. This is a standard for
spaceo-temporal data assets, and indexes massive amounts of cloud-stored data.

.. _Pangeo: http://pangeo.io/

Anaconda Enterprise
~~~~~~~~~~~~~~~~~~~

Intake will be the basis of the data access and cataloging service within
`Anaconda Enterprise`_, running as a micro-service in a container, and offering data
source definitions to users. The access control, who gets to see which data-set,
and serving of credentials to be able to read from the various data storage services,
will all be handled by the platform and be fully configurable by admins.

.. _Anaconda Enterprise: https://www.anaconda.com/enterprise/

National Center for Atmospheric Research
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

NCAR has developed `intake-esm`_, a mechanism for creating file-based Intake catalogs
for climate data from project efforts such as the `Coupled Model Intercomparison Project (CMIP)`_
and the `Community Earth System Model (CESM) Large Ensemble Project`_.
These projects produce a huge of amount climate data persisted on tape, disk storage components
across multiple (of the order ~300,000) netCDF files. Finding, investigating, loading these files into data array containers
such as `xarray` can be a daunting task due to the large number of files a user may be interested in.
``Intake-esm`` addresses this issue in three steps:

- `Datasets Collection Curation`_ in form of YAML files. These YAML files provide information about data locations,
  access pattern,  directory structure, etc. ``intake-esm`` uses these YAML files in conjunction with file name templates
  to construct a local database. Each row in this database consists of a set of metadata such as ``experiment``,
  ``modeling realm``, ``frequency`` corresponding to data contained in one netCDF file.

.. code-block:: python

   col = intake.open_esm_metadatastore(collection_input_definition="GLADE-CMIP5")


- Search and Discovery: once the database is built, ``intake-esm`` can be used for searching and discovering
  of climate datasets by eliminating the need for the user to know specific locations (file path) of
  their data set of interest:

.. code-block:: python

   cat = col.search(variable=['hfls'], frequency='mon', modeling_realm='atmos', institute=['CCCma', 'CNRM-CERFACS'])

- Access: when the user is satisfied with the results of their query, they can ask ``intake-esm``
  to load the actual netCDF files into xarray datasets:

.. code-block:: python

   dsets = cat.to_xarray(decode_times=True, chunks={'time': 50})

.. _intake-esm: https://github.com/NCAR/intake-esm
.. _Datasets Collection Curation: https://github.com/NCAR/intake-esm-datastore
.. _Coupled Model Intercomparison Project (CMIP): https://www.wcrp-climate.org/wgcm-cmip
.. _Community Earth System Model (CESM) Large Ensemble Project: http://www.cesm.ucar.edu/projects/community-projects/LENS/

Brookhaven Archive
~~~~~~~~~~~~~~~~~~

The `Bluesky`_ project uses Intake to dynamically query a MongoDB instance, which
holds the details of experimental and simulation data collections, to return a
custom Catalog for every query. Data-sets can then be loaded into python, or the original
raw data can be accessed ...

.. _Bluesky: https://github.com/bluesky/intake-bluesky

Zillow
~~~~~~

Zillow is developing Intake to meet the needs of their datalake access layer (DAL),
to encapsulate the highly hierarchical nature of their data. Of particular importance,
is the ability to provide different version (testing/production, and different
storage formats) of the same logical dataset, depending on
whether it is being read on a laptop versus the production infrastructure ...

Intake Server
~~~~~~~~~~~~~

The server protocol (see :ref:`server`) is simple enough that anyone can write their
own implementation with full customisation and behaviour. In particular, auth and
monitoring would be essential for a production-grade deployment.
