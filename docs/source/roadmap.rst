.. _roadmap:

Roadmap
=======

Some high-level work that we expect to be achieved on the time-scale of months. This list
is not exhaustive, but rather aims to whet the appetite for what Intake can be in the future.

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

The next type that we would specifically like to consider is machine learning
model artifacts.

Streaming Source
----------------

Many data sources are inherently time-sensitive and event-wise. These are not covered well by existing
Python tools, but the ``streamz`` library may present a nice way to model them. From the Intake point of
view, the task would be to develop a streaming type, and at least one data driver that uses it.

The most obvious place to start would be read a file: every time a new line appears in the file, an event
is emitted. This is appropriate, for instance, for watching the log files of a web-server, and indeed could
be extended to read from an arbitrary socket.

Streamz has seen renewed development recently and a new version is coming soon.

Server publish hooks
--------------------

To add API endpoints to the server, so that a user (with sufficient privilege) can post data
specifications to a running server, optionally saving the specs to a catalog server-side. Furthermore,
we will consider the possibility of being able to upload and/or transform data
(rather than refer to it in a third-party location), so that you would have a one-line "publish"
ability from the client.

Simplify dependencies and class hierarchy
-----------------------------------------

We would like the make it easier to write Intake drivers which don't need any
persist or GUI functionality, and to be able to install Intake core
functionality (driver registry, data loading and catalog traversal) without
needing many other packages at all.
