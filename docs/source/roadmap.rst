.. _roadmap:

Roadmap
=======

Some high-level work that we expect to be achieved ont he time-scale of months. This list
is not exhaustive, but
rather aims to whet the appetite for what Intake can be in the future.

Since Intake aims to be a community of data-oriented pythoneers, nothing written here is laid in
stone, and users
and devs are encouraged to make their opinions known!

Integration with Apache Spark
-----------------------------

The spark ecosystems and Intake will co-operate nicely! Firstly, Spark sources (i.e., names tables) will become
standard data sources, so that the data can be streamed from Spark to a python process, and the data-sets referenced
in a catalog as usual. These data-sets will necessarily be data-frame type, although an RDD-to-sequential method
may also be possible

Later, automatic streaming of data *into* Spark should be possible also, with a `to_spark()` method appearing on
data-frame (and maybe sequence, later) type sources.

Derived Data-sets
-----------------

Often, we can conceive of a data-type as being a modified version of another data-type. For example:
the "csv" plugin produced data-frames from a set of files in the CSV format, while another plugin
takes data-frames with a particular set of fields as input, and produces new data-frames based on some
model predictions.

Rather than allow a general pipeline with arbitrary code specified in catalogues, we aim to allow
the creation of arbitrary *plugins*, where the inputs are the outputs of other data-sources. This
way, the logic stays in the code of the plugin, which can be distributed as python/conda packages as
usual, but a path is in place to generate "second-order" data products. Naturally, such derived
plugins ought to be thorough about describing the process in the metadata of the resultant data-source.
