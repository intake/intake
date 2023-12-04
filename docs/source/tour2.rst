Package Tour
============

Almost all new functionality in Take2 is within ``intake.readers``. Must of the functionality
from V1 should still work as it did before.

intake.config
-------------

Managing and persisting the config.

intake.readers.catalogs
-----------------------

Readers that produce catalogs: a few interesting serice endpoins:

- Tiled
- SQL
- STAC (including paramterised search)
- THREDDS
- NASA Earthdata

and some collections of example data

- huggingface hub
- SKLearn examples
- torch datasets
- TF datasets

intake.readers.convert
----------------------

Classes to convert between data representations without changing the data. Each
converter specifies which types it acts on on, and what it produces.

Includes the Pipeline class used to store a sequence of steps, and a couple of utility
functions for plotting making a graph of the available conversions and finding the shortest
route from one type to another.

intake.readers.datatypes
------------------------

All of the data prescription classes, subclassed from BaseData. Defines the minimum
required information for an instance, and some ways to guess a type from a URL.

intake.readers.entry
--------------------

Classes for the descriptions of data and readers that live inside catalogs, and the
Catalog class itself.

intake.readers.importlist
-------------------------

How modules get imported when intake itself is imported; this is how subclasses of
BaseData, BaseReader and BaseConverter are "registered", rather than relying
exclusively on entrypoints.

intake.readers.metadata
-----------------------

A loose descriptin of the fields expected in a metadata dictionary.

intake.readers.mixins
---------------------

The magic that makes reader[..] and reader.<> work.

intake.readers.namespaces
-------------------------

Set of functions within a few popular packages, such as numpy, that you might expect
to automatically be available for tab-completion of a numpy-producing reader, something
like ``reader.np.abs`` would find the ``np.abs`` function and apply it.

intake.readers.output
---------------------

Converters specialised for producing outputs, normally by side-effect. Most
produce data objects, which you can also put in a catalog.

intake.readers.readers
----------------------

The BaseReader class and all the readers derived from it. These are the things that do
the actual loading of data at runtime. Each one specified which datatype it can read,
what imports must be available, and what it produces. The ``doc()`` method Intake-specific
information, if any, and the docstring of the (main) function used for loading.

intake.readers.transform
------------------------

Converters which actually change the data, but not normally the representation. The simplest
would be column selection from a dataframe.

intake.readers.user_parameters
------------------------------

A few types that can be used to template data and reader descriptions in a Catalog. The extensible
type system allows for simple verification, and may in the future expand to something like
``param`` or ``pydantic``.
