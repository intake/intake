.. _roadmap:

Roadmap
=======

Some high-level work that we expect to be achieved ont he time-scale of months. This list
is not exhaustive, but
rather aims to whet the appetite for what Intake can be in the future.

Since Intake aims to be a community of data-oriented pythoneers, nothing written here is laid in
stone, and users and devs are encouraged to make their opinions known!

Broaden the coverage of formats
-------------------------------

Data-type drivers are easy to write, but still require some effort, and therefore reasonable
impetus to get the work done. Conversations over the coming months can help determine the
drivers that should be created by the Intake team, and those that might be contributed by the
community.


Streaming Source
----------------

Many data sources are inherently time-sensitive and event-wise. These are not covered well by existing
Python tools, but the ``streamz`` library may present a nice way to model them. From the Intake point of
view, the task would be to develop a streaming type, and at least one data driver that uses it.

The most obvious place to start would be read a file: every time a new line appears in the file, an event
is emitted. This is appropriate, for instance, for watching the log files of a web-server, and indeed could
be extended to read from an arbitrary socket.


Persistence
-----------

Intake is not in the business or *writing* data. However, each of the container types do lend themselves
to a particular on-disc format, wherever they came from. This proposal is to allow for a persist method
on every source, which loads the data and saves it to a configured storage location (local or remote),
and automatically adds an entry to some "persisted data" catalog. Such entries may be time-restricted and
eventually expire, or perhaps automatically renew themselves.

This is the counterpart to caching, which involves making local copies of remote files. Here we can save
anything, for example the output of an expensive SQL query.


Next-generation GUI
-------------------

The jupyter-widgets GUI is useful and simple, but we can do better. See the `long form proposal`_.

.. _long form proposal: https://github.com/intake/intake/issues/225


Catalog services
----------------

We are experimenting with reflecting external catalog-like data servers as Intake catalogs, so that the
familiar API can be used for all the disparate services. See for example `this discussion`_.

.. _this discussion: https://github.com/intake/intake/issues/224

Use DAT as a cache service
--------------------------

The [DAT protocol](https://datproject.org/)