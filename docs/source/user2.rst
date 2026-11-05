.. catalog_user:

Catalog User
============

So someone has sent you an Intake URL or other way to load a catalog. What happens next?
First, create a small local catalog so that you can run this example without a
remote data service or credentials. Install ``intake`` and ``pandas`` and run
the following in a working directory where you can create files:

.. code-block:: python

    import operator
    import string

    import intake
    import pandas as pd

    pd.DataFrame({"a": ["ho", "hi"], "b": [4, 5]}).to_csv("intake_1.csv")
    data = intake.readers.datatypes.CSV("{CATALOG_DIR}/intake_1.csv")
    reader = intake.readers.readers.PandasCSV(data)
    sample = intake.Catalog()
    sample["tute"] = reader
    column = reader.apply(operator.getitem, "a", output_instance="pandas:Series")
    sample["capitals"] = column.map(string.capwords)
    sample["inverted"] = reader.sort_values("b", ascending=False)
    sample["multi"] = sample.tute.assign(c=sample.capitals)
    sample.to_yaml_file("intake_1.yaml")

    cat = intake.from_yaml_file("intake_1.yaml")

(the same data and reader operations are used in the `tutorial notebook`_)

.. _tutorial notebook: https://github.com/intake/intake/blob/master/examples/Take2.ipynb

Displaying the catalog shows that it has four named datasets and some automatically
populated "user parameters".

Interesting attributes of the catalog are:

- ``cat.data``: full description of the original datasets

.. code-block:: python

    cat.data

- ``cat.entries``: readers, ways to load the data

.. code-block:: python

    cat.entries

- ``cat.aliases``: names to associate with readers or data (these are the ones used with tab-completion)

.. code-block:: python

    cat.aliases

- ``cat.user_parameters``: values that can be used in templated values (see below)

.. code-block:: python

    cat.user_parameters

``CATALOG_PATH`` and ``CATALOG_DIR`` contain the absolute path of the YAML file
and its containing directory. ``STORAGE_OPTIONS`` is empty for this local example.


You can even get an overall view of everything in the catalog using ``cat.to_dict()``, which gives you
back essentially the same information as was contained in the YAML file we read the catalog from.
Also notice, that there is metadata associated with the whole catalog, and each of the
data and reader descriptions. All the readers depend on the one dataset ("multi" depends on
it twice) and all are Pipelines (with "steps") except "tute". The ``capitals``
pipeline selects column ``a`` as a Series before mapping ``string.capwords`` over it.

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

The data URL now points to ``intake_1.csv`` in the directory containing the catalog.


.. note::

    We will work on the best way to represent the various instances, especially in the notebook.
    For the time being, you can always use the ``.pprint()`` method, or introspect the
    instance's attributes.

We notice that this is NOT exactly the same as the entry in the original catalog with name
"tute". In particular: the reader is a concrete instance of a subclass of
:class:`intake.readers.readers.BaseReader`,
it contains the data definition it referenced and the URL of which has been expanded to the
full local path.

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

Returning to the absolute data path in the reader instance above. This was created from
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

Parameters in a child catalog
-----------------------------

A catalog can contain a reader for another catalog. Load that child catalog
before setting parameters defined inside it. For example, use the sample data
above in a child catalog with a parameter controlling how many CSV rows to read:

.. code-block:: python

    from intake.readers.user_parameters import SimpleUserParameter

    child = intake.Catalog(
        user_parameters={"row_count": SimpleUserParameter(default=1, dtype=int)}
    )
    child["sample"] = intake.readers.readers.PandasCSV(data, nrows="{row_count}")
    child.to_yaml_file("child.yaml")

    parent = intake.Catalog()
    parent["child"] = intake.readers.readers.YAMLCatalogReader(
        intake.readers.datatypes.YAMLFile("{CATALOG_DIR}/child.yaml")
    )
    parent.to_yaml_file("parent.yaml")

    parent = intake.from_yaml_file("parent.yaml")
    child = parent.child.read()
    result = child(row_count=2).sample.read()
    assert result["a"].tolist() == ["ho", "hi"]

``parent.child`` is a reader that produces a catalog; ``parent.child.read()``
is the child catalog itself. Calling ``child(row_count=2)`` returns a catalog
with that parameter overridden, leaving the original default unchanged. Setting
``row_count`` on the parent does not automatically pass it into the child catalog.
