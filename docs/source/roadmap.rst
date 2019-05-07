.. _roadmap:

Roadmap
=======

Some high-level work that we expect to be achieved ont he time-scale of months. This list
is not exhaustive, but
rather aims to whet the appetite for what Intake can be in the future.

Since Intake aims to be a community of data-oriented pythoneers, nothing written here is laid in
stone, and users and devs are encouraged to make their opinions known!

See also the [wiki page](https://github.com/intake/intake/wiki/Community-News) on latest Intake
community news.

Broaden the coverage of formats
-------------------------------

Data-type drivers are easy to write, but still require some effort, and therefore reasonable
impetus to get the work done. Conversations over the coming months can help determine the
drivers that should be created by the Intake team, and those that might be contributed by the
community.

The next type that we would specifically like to consider is machine learning model artefacts.

Streaming Source
----------------

Many data sources are inherently time-sensitive and event-wise. These are not covered well by existing
Python tools, but the ``streamz`` library may present a nice way to model them. From the Intake point of
view, the task would be to develop a streaming type, and at least one data driver that uses it.

The most obvious place to start would be read a file: every time a new line appears in the file, an event
is emitted. This is appropriate, for instance, for watching the log files of a web-server, and indeed could
be extended to read from an arbitrary socket.

Streamz has seen renewed development recently and a new version is coming soon.

GUI interactive plotting
------------------------

The new `Panel`-based GUI can act as a standalone data catalog browser. It has the ability,
currently, to display the "builtin" plots included with the data source specification (which
are generally interactive). We would
like to develop more arbitrary interactions, so the user can choose the type of plot (or
data table) to show,
the fields to build from and display options for various data types; and this should include
options on how to sample from the data.

It would be good to be able to save these hand-made plot definitions into the catalogs containing
the data source specs.

Server publish hooks
--------------------

To add API endpoints to the server, so that a user (with sufficient privilege) can post data
specifications to a running server, optionally saving the specs to a catalog server-side. Furthermore,
we will consider the possibility of being able to upload and/or transform data
(rather than refer to it in a third-party location), so that you would have a one-line "publish"
ability from the client.
