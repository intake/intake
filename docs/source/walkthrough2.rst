Walkthrough
===========

Preamble
--------

Intake is a bunch of _readers_. A reader is a class that encodes how to load some
particular data, including all the arguments that will be passed to some third-party
package. Thus, you can encode anything from the simplest ``pd.read_csv`` call to
far more complex things. You can also act on readers, to record a set of
operations known as a Pipeline. A Pipeline is just a special kind of reader.

Catalogs contain readers and data definitions. Readers can and should be saved in Catalogs,
which can in the simplest case be saved as text files, but can also be generated from
may data services. Catalogs and readers are the central themes of intake.

.. note::

    "Load" means make a machine representations: many data containers may be lazy,
    where you get an object on which you can act, but the data is only read on
    demand for actions that rerquire it.


Simple Example
--------------

Let's consider a typical Pandas line to read a CSV file. Although there are many ways to
load data in python, this is one of the most common.

.. code-block:: python

    import pandas as pd
    url = "s3://mymdtemp/intake_1.csv"
    df = pd.read_csv(url, storage_options={"anon": True}, usecols=[1, 2])

Simple enough, so long as you have ``fsspec`` and ``s3fs`` installed. Note the use of extra
options to the storage backend (this file is public) and up-front column selection.

To encode this with intake, you can do

.. code-block:: python

    import intake
    reader = intake.reader_from_call(
        'df = pd.read_csv(url, storage_options={"anon": True}, usecols=[1, 2])')

Where you could have used ``_i``, the IPython shorthand for "the last line of input" instead of
the copy/pasted line. The "reader" object encodes all the things we asked for, and we
can execute the process by calling ``reader.read()`` to get the same result as before. So
what's the point?

We can put this reader into a catalog:

.. code-block:: python

    cat = intake.entry.Catalog()
    cat["tute"] = reader
    cat.to_yaml_file("intake_1.yaml")

where the path could be anyplace you can write to, such as a local file. This can be the
whole of the "catalog producer" workflow.

A public version has been put alongside the CSV, so as the data consumer, you can read this
wherever you are. The following could be the whole of a data consumer workflow:

.. code-block:: python

    import intake
    cat = intake.from_yaml_file("s3://mymdtemp/intake_1.yaml", anon=True)
    cat.tute.read()

Note that ``cat.tute`` has a lot of optional metadata that we have not filled out. The point is
not to know every time which specific dataset you need (although that happens too), but a way
to view all of your available data and find the right one for your job before loading it.

Slightly less Simple
--------------------

Let's do trivial transforms to our trivial dataset. Continuing from above:

.. code-block:: python

    cat["capitals"] = reader.a.str.capitalize()
    cat["inverted"] = reader.sort_values("b", ascending=False)
    cat.to_yaml_file("intake_1.yaml")

Again, this could be persisted anywhere, but the path above includes all three datasets:

.. code-block:: python

    import intake
    cat = intake.from_yaml_file("s3://mymdtemp/intake_1.yaml", anon=True)
    list(cat)  # -> ['capitals', 'inverted', 'tute']

Now we have three datasets all based off the same original file,

Complex Example
---------------

The following code performs a rather typical workflow, recreating the "persist" functionality
in V1 (using only standard blocks, no special code). This is somewhat verbose and explicit, for
the sake of clarity.

.. code-block:: python

    from intake.readers.readers import Condition, PandasCSV, PandasParquet, FileExistsReader

    fn = f"{tmpdir}/file.parquet"
    data = intake.readers.datatypes.CSV(url=dataframe_file)
    part = PandasCSV(data)

    output = part.PandasToParquet(url=fn).transform(PandasParquet)
    data2 = intake.readers.datatypes.Parquet(url=fn)
    cached = PandasParquet(data=data2)
    reader2 = Condition(cached, if_false=output, condition=FileExistsReader(data2))

The pipeline can be described as:
- there is a CSV file, ``dataframe_file``
- there may be a parquet version of this, ``fn``
- if the parquet file does not exist, load the CSV using pandas, save it to parquet and load that
- if the parquet file already exists, load that without looking at the CSV.

There are of course many ways that one might achieve this and more complex "conditions" for when
to run the conversion pipeline. However, the ``reader2`` object encodes the whole thing, and can
be safely stored in a catalog. A user can then use this standard condition, choose to remake the
parquet, or just load the CSV without accessing the parquet at all. It would be reasonable to
update the metadata of ``data`` or the readers to show the expected columns types and row count
(if they are not expected to change).
