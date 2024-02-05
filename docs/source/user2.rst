.. catalog_user:

Catalog User
============

So someone has sent you an Intake URL or other way to load a catalog. What happens next?
Let's do the simplest thing and load a public catalog

.. code-block:: python

    cat = intake.from_yaml_file("s3://mymdtemp/intake_1.yaml", anon=True)

(this is the same catalog as is made in the example `tutorial noteboook`_)

.. _tutorial noteboook: https://github.com/intake/intake/blob/master/examples/Take2.ipynb

Displaying the catalog shows that it has four named datasets and some automatically
populated "user parameters".

Interesting attributes of the catalog are:

- ``cat.data``: full description of the original datasets

.. code-block:: python

    {'7e0b327a50eef58d': DataDescription type intake.readers.datatypes:CSV
      kwargs {'metadata': {}, 'storage_options': {'anon': True}, 'url': '{CATALOG_DIR}/intake_1.csv'}}

- ``cat.entries``: readers, ways to load the data

.. code-block:: python

    {'capitals': Entry for reader: intake.readers.convert:Pipeline
      kwargs: {'out_instances': ['pandas:DataFrame', 'pandas:DataFrame', 'pandas:DataFrame', 'pandas:DataFrame'],
               'steps': [
                    ['{data(tute)}', [], {}],
                    ['{func(intake.readers.transform:Method)}', [], {'method_name': 'a'}],
                    ['{func(intake.readers.transform:Method)}', [], {'method_name': 'str'}],
                    ['{func(intake.readers.transform:Method)}', [], {'method_name': 'capitalize'}]]}
      producing: pandas:DataFrame,
     'inverted': Entry for reader: intake.readers.convert:Pipeline
      kwargs: {'out_instances': ['pandas:DataFrame', 'pandas:DataFrame'],
               'steps': [
                    ['{data(tute)}', [], {}],
                    ['{func(intake.readers.transform:Method)}', [], {'args': ['b'], 'ascending': False, 'method_name': 'sort_values'}]]}
      producing: pandas:DataFrame,
     'multi': Entry for reader: intake.readers.convert:Pipeline
      kwargs: {'out_instances': ['pandas:DataFrame', 'pandas:DataFrame'],
               'steps': [
                    ['{data(tute)}', [], {}],
                    ['{func(intake.readers.transform:Method)}', [], {'c': '{data(capitals)}', 'method_name': 'assign'}]]}
      producing: pandas:DataFrame,
     'tute': Entry for reader: intake.readers.readers:PandasCSV
      kwargs: {'data': '{data(7e0b327a50eef58d)}'}
      producing: pandas:DataFrame}

- ``cat.aliases``: names to associate with readers or data (these are the ones used with tab-completion)

.. code-block:: python

    {'capitals': 'capitals',
     'inverted': 'inverted',
     'multi': 'multi',
     'tute': 'tute'}

- ``cat.user_parameters``: values that can be used in templated values (see below)

.. code-block:: python

    {'CATALOG_PATH': 's3://mymdtemp/intake_1.yaml',
     'CATALOG_DIR': 's3://mymdtemp',
     'STORAGE_OPTIONS': {'anon': True}}


You can even get an overall view of everything in the catalog using ``cat.to_dict()``, which gives you
back essentially the same information as was contained in the YAML file we read the catalog from.
Also notice, that there is metadata associated with the whole catalog, and each of the
data and reader descriptions. All the readers depend on the one dataset ("multi" depends on
it twice) and all are Pipelines (with "steps") except "tute".

Key Concepts
------------

A few definitions that will help you:

- data: a set of numbers of various forms, which can be used to infer information
  about some domain

- dataset: a specific delimited amount of data, often a single file, a directory of
  files or a single request or query to some service. The output of any Intake reader
  is also a "dataset", as represented in a live python session

- data: the basic information needed to uniquely identify a dataset, such
  as data type, URL/paths, server location, query. Intake supports many data types (:ref:`data`).
  A description ought to also contain descriptive information in its metadata.

- reader: how a given dataset should be handled/loaded. This is more specific than the data
  itself, since there may be many different ways to read the data. For instance, CSVs are a
  very common and simple data format, and virtually all (table-oriented) data packages can read them.

- pipeline: a sequence of operations on a dataset. In Intake, this is just a type of reader, although
  it is possible to refer to the output of any particular stage.

- catalog: a collection of datasets and their reader descriptions. Each dataset may be referred to by
  multiple readers, and a reader may refer to multiple datasets, although the latter is less common
  (think of JOIN operations).

- templates, user-parameters: in the catalog definition of the one dataset, you will notice special
  syntax for part of the URL value to be filled in. See below for how to work with this.

Reader API
----------

Before accessing any of the entries in a catalog, you should introspect them to see if it is
what you are after. There should be descriptive text, other metadata and of course the contents
of the data/readers, as shown above. It is important to note, that extracting readers (the next
step) already comes with security implications, such as evaluating environment variables and
making imports. The "allow_*" keys in the intake configuration, ``intake.conf`` define what
is generally allowed.

As an end-user, you will generally interact with readers. Get them from the catalog by attribute
access or item access; the latter is required where the name is not a valid python identifier
or conflicts with a method. The following two line are exactly equivalent:

.. code-block::

    reader = cat.tute
    reader = cat["tute"]
    reader.pprint()

    {'kwargs': {'data': {'url': 's3://mymdtemp/intake_1.csv',
                     'storage_options': {'anon': True},
                     'metadata': {}}},
     'metadata': {},
     'output_instance': 'pandas:DataFrame'}


.. note::

    We will work on the best way to represent the various instances, especially in the notebook.
    For the time being, you can always use the ``.pprint()`` method, or introspect the
    instance's attributes.

We notice that this is NOT exactly the same as the entry in the original catalog with name
"tute". In particular: the reader is a concrete instance of a subclass of
:class:`intake.readers.readers.BaseReader`,
it contains the data definition it referenced and the URL of which has been expanded to the
full "s3://..".

The most obvious thing to do to a reader is read: this is, after all, what they are for. We
already know to expect an output type a pandas DataFrame. ``reader.doc()`` provides the
docstring of the target function, in this case ``read_csv()``. You can
pass extra or override arguments, with exactly the same names and meaning as the
original docstring (some readers might provide extra functionality or possibilities).

.. code-block:: python

    reader.read()

       Unnamed: 0   a  b
    0           0  ho  4
    1           1  hi  5

    reader(index_col=[0]).read()

        a  b
    0  ho  4
    1  hi  5

For large datasets, you may try ``.discover()`` instead, which is generally a small subset
of the data, depending on the format and library. For small datasets like this one, you get
exactly the same output.

Templates
---------

Returning to the mysterious "s3://" URL in the reader instance above. This was created from
the URL "{CATALOG_DIR}/intake_1.csv" using templating. You may recall that the catalog had
a user_parameter of this name, whose value was auto-populated from the URL we used to read
the catalog file. This means that the data file and catalog describing it could be moved
together to a new location without having to edit the catalog. On the other hand, if the
URL were not templated, moving/copying the catalog would still refer to the original
data location (sometimes this is what you want).

This particular user_parameter was global to the catalog, and to assign a new value before
templating, you would do

.. code-block:: python

    cat2 = cat(CATALOG_DIR="new_value")

(so ``cat2.tute`` would not have a different data URL and no longer load!). It is also
possible to have parameters associated with the data description and/or specific readers,
and for any parameter to be used in multiple places. They can also have specific types,
defaults and constraints/choices. If a template refers to a parameter that is missing
or has no value set, it will be left unchanged, and the data in question will probably not load.
