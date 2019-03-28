Use Cases
=========

Here follows a list of specific things that people may want to get done, and
details of how Intake can help. The details of how to achieve each of these
activities can be found in the rest of the detailed documentation.

**I want to...**
~~~~~~~~~~~

Avoid copy&paste of blocks of code for accessing data
-----------------------------------------------------

This is a very common pattern, if you want to load some specific data, to
find someone, perhaps a colleague, who has accessed it before, and copy that
code. Such a practice is extremely error prone, and cause a proliferation of
copies of code, which may evolve over time, with various versions simultaneously
in use.

Intake separates the concerns of data-source specification from code. The
specs are stored separately, and all users can reference the one and only
authoritative definition, whether in a shared file, a service visible to
everyone or by using the Intake server. This spec can be updated so that
everyone gets the current version instead of relying on outdated code.

Version control data sources
----------------------------

Version control (e.g., using ``git``) is an essential practice in modern
software engineering and data science. It ensures that the change history is
recorded, with times, descriptions and authors along with the changes themselves.

When data is specified using a well-structured syntax such as YAML, it can
be checked into a version controlled repository in the usual fashion. Thus, you
can bring rigorous practices to your data as well as your code.

If using conda packages to distribute data specifications, these come with a
natural internal version numbering system, such that users need only do
``conda update ...`` to get the latest version.

"Install" data
--------------

Often, finding and grabbing data is a major hurdle to productivity. People may be
required to download artifacts from various places or search through storage
systems to find the specific thing that they are after. One-line commands which
can retrieve data-source specifications or the files themselves can be a massive
time-saver. Furthermore, each data-set will typically need it's own code to
be able to access it, and probably additional software dependencies.

Intake allows you to build ``conda`` packages, which can include catalog files
referencing online resources, or to include data files directly in tha package.
Whether uploaded to ``anaconda.org`` or hosted on a private enterprise channel,
getting the data becomes a single ``conda install ...`` command, whereafter
it will appear as an entry in ``intake.cat``. The conda package brings versioning
and dependency declaration for free, and you can include any code that may be
required for that specific data-set directly in the package too.

Update data specifications in-place
-----------------------------------

Individual data-sets often may be static, but commonly, the "best" data to get a
job done changes with time as new facts emerge. Conversely, the very same data
might be better stored in a different format which is, for instance, better-suited
to parallel access in the cloud. In such situations, you really don't want to force
all the data scientists who rely on it to have their code temporarily broken and
be forced to change this code.

By working with a catalog file/service in a fixed shared location, it is possible to
update the data source specs in-place. When users now run their code, they will get
the latest version. Because all Intake drivers have the same API, the code using the
data will be identical and not need to be changed, even when the format has been
updated to something more optimised.

Access data stored on cloud resources
-------------------------------------

Services such as AWS S3, GCS and Azure Datalake (or private enterprise variants of
these) are increasingly popular locations to amass large amounts of data. Not only
are they relatively cheap per GB, but they provide long-term resilience, metadata
services, complex access control patterns and can have very large data throughput
when accessed in parallel by machines on the same architecture.

Intake comes with integration to cloud-based storage out-of-the box for most of the
file-based data formats, to be able to access the data directly in-place and in
parallel. For the few remaining cases where direct access is not feasible, the
caching system in Intake allows for download of files on first use, so that
all further  access is much faster.

Work with "Big Data"
--------------------

The era of Big Data is here! The term means different things to different people,
but certainly implies that an individual data-set is too large to fit into the
memory of a typical workstation computer (>>10GB). Nevertheless, most data-loading
examples available use functions in packages such as ``pandas`` and expect to
be able to produce in-memory representations of the whole data. This is clearly a
problem, and a more general answer should be available aside from "get more memory
in your machine".

Intake integrates with ``Dask`` and ``Spark``, which both offer out-of-core
computation (loading the data in chunks which fit in memory and aggregating result)
or can spread their work over a cluster of machines, effectively making use of the
shared memory resources of the whole cluster. Dask integration is built into the
majority of the the drivers and exposed with the ``.to_dask()`` method, and Spark
integration is available for a small number of drivers with a similar ``.to_spark()``
method, as well as directly with the ``intake-spark`` package.

Intake also integrates with many data services which themselves can perform big-data
computations, only extracting the smaller aggregated data-sets that *do* fit into
memory for further analysis. Services such as SQL systems, ``solr``, ``elastic-search``,
``splunk``, ``accumulo`` and ``hbase`` all can distribute the work required to fulfill
a query across many nodes of a cluster.

Find the right data-set
-----------------------

Browsing for the data-set which will solve a particular problem can be hard, even
when the data have been curated and stored in a single, well-structured system. You
do *not* want to rely on word-of-mouth to specify which data is right for which job.

