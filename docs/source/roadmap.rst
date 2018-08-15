.. _roadmap:

Roadmap
=======

Some high-level work that we expect to be achieved ont he time-scale of months. This list is not exhaustive, but
rather aims to whet the appetite for what Intake can be in the future.

Since Intake aims to be a community of data-oriented pythoneers, nothing written here is laid in stone, and users
and devs are encouraged to make their opinions known!

Browser GUI
-----------

To develop a panel plug-in for jupyter-lab, which displays the built-in datasets, can connect to an Intake Server or
load catalog files. Each entry in the display would give a little information about itself, and a button would allow
for a line of code to load data-set to be injected in the currently-active notebook.

Integration with Apache Spark
-----------------------------

The spark ecosystems and Intake will co-operate nicely! Firstly, Spark sources (i.e., names tables) will become
standard data sources, so that the data can be streamed from Spark to a python process, and the data-sets referenced
in a catalog as usual. These data-sets will necessarily be data-frame type, although an RDD-to-sequential method
may also be possible

Later, automatic streaming of data *into* Spark should be possible also, with a `to_spark()` method appearing on
data-frame (and maybe sequence, later) type sources.

Streaming of array and complex data
-----------------------------------

Currently, only sequence and dataframe types can stream from an Intake server. One particular wish is to be able to
stream netCDF/HDF5 files, which are array containers, because the loaders of these data types are limited to local
files, and so particularly inconvenient for cloud storage. Xarray loaders are already available via the
``intake-xarray`` package, but many array-type data sources exist (such as images) that this would be very handy for.

