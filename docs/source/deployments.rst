Deployment Scenarios
--------------------

In the following sections, we will describe some of the ways in which Intake is used in real
production systems. These go well beyond the typical YAML files presented in the quickstart
and examples sections, which are necessarily short and simple, and do not demonstrate the
full power of Intake.

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

NCAR has developed intake-esm, a mechanism for creating file-based Intake catalogs
out of data prescriptions stored in science standard formats like CMIP and CESM and
accessed via openDAP ...

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
