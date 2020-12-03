.. _usecases:

Use Cases - I want to...
========================

Here follows a list of specific things that people may want to get done, and
details of how Intake can help. The details of how to achieve each of these
activities can be found in the rest of the detailed documentation.

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
time-saver. Furthermore, each data-set will typically need its own code to
be able to access it, and probably additional software dependencies.

Intake allows you to build ``conda`` packages, which can include catalog files
referencing online resources, or to include data files directly in that package.
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

Intake catalogs allow for self-description of data-sets, with simple text and
arbitrary metadata, with a consistent access pattern. Not only can you list the data
available to you, but you can find out what exactly that data represents, and
the form the data would take if loaded (table versus list of items, for example). This extra
metadata is also searchable: you can descend through a hierarchy of catalogs with
a single search, and find all the entries containing some particular keywords.

You can use the Intake GUI to graphically browse through your available data-sets or
point to catalogs available to you, look through the entries listed there and get
information about each, or even show a sample of the data or quick-look plots. The GUI
is also able to execute searches and browse file-systems to find data artifacts of
interest. This same functionality is also available via a command-line interface
or programmatically.

Work remotely
-------------

Interacting with cloud storage resources is very convenient, but you will not want to
download large amounts of data to your laptop or workstation for analysis. Intake
finds itself at home in the remote-execution world of jupyter and Anaconda Enterprise
and other in-browser technologies. For instance, you can run the Intake GUI either as a
stand-alone application for browsing data-sets or in a notebook for full analytics,
and have all the runtime live on a remote machine, or perhaps a cluster which is
co-located with the data storage. Together with cloud-optimised data formats such
as parquet, this is an ideal set-up for processing data at web scale.

Transform data to efficient formats for sharing
-----------------------------------------------

A massive amount of data exists in human-readable formats such as JSON, XML and CSV,
which are not very efficient in terms of space usage and need to be parsed on
load to turn into arrays or tables. Much faster processing times can be had with
modern compact, optimised formats, such as parquet.

Intake has a "persist" mechanism to transform any input data-source into the format
most appropriate for that type of data, e.g., parquet for tabular data. The persisted
data will be used in preference at analysis time, and the schedule for updating from
the original source is configurable. The location of these persisted data-sets can
be shared with others, so they can also gain the benefits, or the "export" variant
can be used to produce an independent version in the same format, together with a
spec to reference it by; you would then share this spec with others.

Access data without leaking credentials
---------------------------------------

Security is important. Users' identity and authority to view specific data should be
established before handing over any sensitive bytes. It is, unfortunately, all too
common for data scientists to include their username, passwords or other credentials
directly in code, so that it can run automatically, thus presenting a potential
security gap.

Intake does not manage credentials or user identities directly, but does provide hooks
for fetching details from the environment or other service, and using the values in
templating at the time of reading the data. Thus, the details are not included in
the code, but every access still requires for them to be present.

In other cases, you may want to require the user to provide their credentials every
time, rather that automatically establish them, and "user parameters" can be specified
in Intake to cover this case.

Establish a data gateway
------------------------

The Intake server protocol allows you fine-grained control over the set of data sources
that are listed, and exactly what to return to a user when they want to read some of
that data. This is an ideal opportunity to include authorisation checks,
audit logging, and any more complicated access patterns, as required.

By streaming the data through a single channel on the server, rather than allowing
users direct access to the data storage backend, you can log and verify all access
to your data.

Clear distinction between data curator and analyst roles
--------------------------------------------------------

It is desirable to separate out two tasks: the definition of data-source specifications, and
accessing and using data. This is so that those who understand the origins of the data
and the implications of various formats and other storage options (such as chunk-size)
should make those decisions and encode what they have done into specs. It leaves the
data users, e.g., data scientists, free to find and use the data-sets appropriate for
their work and simply get on with their job - without having to learn about various
storage formats and access APIs.

This separation is at the very core of what Intake was designed to do.

Users to be able to access data without learning every backend API
------------------------------------------------------------------

Data formats and services are a wide mess of many libraries and APIs. A large amount of
time can be wasted in the life of a data scientist or engineer in finding out the details
of the ones required by their work. Intake wraps these various libraries, REST APIs and
similar, to provide a consistent experience for the data user. ``source.read()`` will
simply get all of the data into memory in the container type for that source - no
further parameters or knowledge required.

Even for the curator of data catalogs or data driver authors, the framework established by
Intake provides a lot of convenience and simplification which allows each person to
deal with only the specifics of their job.

Data sources to be self-describing
----------------------------------

Having a bunch of files in some directory is a very common pattern for data storage in the
wild. There may or may not be a README file co-located giving some information in a
human-readable form, but generally not structured - such files are usually different
in every case.

When a data source is encoded into a catalog, the spec offers a natural place to describe
what that data is, along with the possibility to provide an arbitrary amount of
structured metadata and to describe any parameters that are to be exposed for user
choice. Furthermore, Intake data sources each have a particular container type, so
that users know whether to expect a dataframe, array, etc., and simple introspection
methods like ``describe`` and ``discover`` which return basic information about the
data without having to load all of it into memory first.

A data source hierarchy for natural structuring
-----------------------------------------------

Usually, the set of data sources held by an organisation have relationships to one another,
and would be poorly served to be provided as a simple flat list of everything available.
Intake allows catalogs to refer to other catalogs. This means, that you can group data
sources by various facets (type, department, time...) and establish hierarchical
data-source trees within which to find the particular data most likely to be of interest.
Since the catalogs live outside and separate from the data files themselves, as many
hierarchy structures as thought useful could be created.

For even more complicated data source meta-structures, it is possible to store all the
details and even metadata in some external service (e.g., traditional SQL tables) with which
Intake can interact to perform queries and return particular subsets of the
available data sources.

Expose several data collections under a single system
-----------------------------------------------------

There are already several catalog-like data services in existence in the world, and
some organisation may have several of these in-house for various different purposes.
For example, an SQL server may hold details of customer lists and transactions, but
historical time-series and reference data may be held separately in archival data formats like
parquet on a file-storage system; while real-time system monitoring is done by a
totally unrelated system such as Splunk or elastic search.

Of course, Intake can read from various file formats and data services. However, it
can also interpret the internal conception of data catalogs that some data services may
have. For example, all of the tables known to the SQL server, or all of the pre-defined
queries in Splunk can be automatically included as catalogs in Intake, and take their
place amongst the regular YAML-specified data sources, with exactly the same usage for
all of them.

These data sources and their hierarchical structure can then be exposed via the
graphical data browser, for searching, selecting and visualising data-sets.

Modern visualisations for all data-sets
---------------------------------------

Intake is integrated with the comprehensive ``holoviz`` suite, particularly ``hvplot``, to 
bring simple yet powerful data visualisations to any Intake data source by using just one
single method for everything. These plots are interactive, and can include server-side
dynamic aggregation of very large data-sets to display more data points than the
browser can handle.

You can specify specific plot types right in the data source definition, to have these
customised visualisations available to the user as simple one-liners known to
reveal the content of the data, or even view the same visuals right in the graphical
data source browser application. Thus, Intake is already an all-in-one data investigation
and dashboarding app.


Update data specifications in real time
---------------------------------------

Intake data catalogs are not limited to reading static specification from
files. They can also execute queries on remote data services and return lists of
data sources dynamically at runtime. New data sources may appear, for example,
as directories of data files are pushed to a storage service, or new tables are
created within a SQL server.

Distribute data in a custom format
----------------------------------

Sometimes, the well-known data formats are just not right for a given data-set,
and a custom-built format is required. In such cases, the code to read the data
may not exist in any library. Intake allows for code to be distributed along
with data source specs/catalogs or even files in a single ``conda`` package.
That encapsulates everything needed to describe and use that particular data,
and can then be distributed as a single entity, and installed with a one-liner.

Furthermore, should the few builtin container types (sequence, array, dataframe)
not be sufficient, you can supply your own, and then build drivers that use it.
This was done, for example, for ``xarray``-type data, where multiple related
N-D arrays share a coordinate system and metadata. By creating this container,
a whole world of scientific and engineering data was opened up to Intake. Creating
new containers is not hard, though, and we foresee more coming, such as
machine-learning models and streaming/real-time data.

Create Intake data-sets from scratch
------------------------------------

If you have a set of files or a data service which you wish to make into a data-set,
so that you can include it in a catalog, you should use the set of functions
``intake.open_*``, where you need to pick the function appropriate for your
particular data. You can use tab-completion to list the set of data drivers you have
installed, and find others you may not yet have installed at :ref:`plugin-directory`.
Once you have determined the right set of parameters to load the data in the manner
you wish, you can use the source's ``.yaml()`` method to find the spec that describes
the source, so you can insert it into a catalog (with appropriate description and
metadata). Alternatively, you can open a YAML file as a catalog with ``intake.open_catalog``
and use its ``.add()`` method to insert the source into the corresponding file.

If, instead, you have data in your session in one of the containers supported by Intake
(e.g., array, data-frame), you can use the ``intake.upload()`` function to save it to
files in an appropriate format and a location you specify, and give you back a data-source
instance, which, again, you can use with ``.yaml()`` or ``.add()``, as above.
