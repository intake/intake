Developers' Package Tour
========================

General Guidelines
------------------

Intake is an open source project, and all development happens on `github`_. Please open issues
or discussions there to talk about problems with the code or request features.

To contribute, you should:

- clone the repo locally
- fork the repo to your personal identity in github using the "fork" button
- run ``pre-commit install`` in the repo
- make changes locally as you see fit, and commit to a new branch
- push the branch to your fork, and follow the prompt to create a Pull Request (PR)

You can expect comments on your PR within a couple of days.

To have a higher chance of having your changes accepted, a concise title description are best,
and ideally new code should be accompanied by tests.

.. _github: https://github.com/intake/intake

Outline
-------

For those interested in Intake Take2, here are the places to look for contributing.
All of the implementation code lives under ``intake.readers``, which was developed for a
while parallel with and without touching Intake's V1 code.
The list below gives summaries of the modules,
and the principle classes themselves are in the :ref:`api2`.


.. autosummary::
    intake.config
    intake.readers.catalogs
    intake.readers.convert
    intake.readers.datatypes
    intake.readers.entry
    intake.readers.importlist
    intake.readers.metadata
    intake.readers.mixins
    intake.readers.namespaces
    intake.readers.output
    intake.readers.readers
    intake.readers.search
    intake.readers.transform
    intake.readers.user_parameters


Creating Datatypes and Readers
------------------------------

Here follows a minimalist complete set of classes to make a complete pipeline.

A typical data/reader implementation in Intake Take 2 is very simple. Here is the CSV
prototype

.. code-block:: python

    from intake.readers import FileData

    class CSV(FileData):
        filepattern = "(csv$|txt$|tsv$)"
        mimetypes = "(text/csv|application/csv|application/vnd.ms-excel)"
        structure = {"table"}

This specified that CSVs live in files (the superclass), which also implies that they may
be local or remote. Further, the block specifies expected URL/filenames and MIME types,
as well as an indicator that this filetype is typically used for tables. All of these attributes
are optional - an instance just contains enough information to unambiguously identify the source
of data. For the case of a CSV dataset, this would be just the URL(s) of the data plus any
extra storage backend parameters. Other data types may have other necessary attributes,
such as a SQL dataset is a combination of server connection string and query.

The pandas CSV reader counterpart looks like this

.. code-block:: python

    from intake.readers import FileReader, datatypes

    class PandasCSV(FileReader):
        imports = {"pandas"}
        output_instance = "pandas:DataFrame"
        storage_options = True
        implements = {datatypes.CSV}
        func = "pandas:read_csv"
        url_arg = "filepath_or_buffer"

This says that:

- the data type is made of files
- the reader requires "pandas" to be installed
- the result will be a DataFrame
- if the URL is remote, fsspec-style storage_options are acceptable
- it can be used on the CSV type from before (only)
- it uses the ``read_csv`` function from the ``pandas`` package
- and that the URL of the data source should be passed using the argument name "filepath_or_buffer" (this information can be found
  from the target function's signature and docstring).

Often a reader is this simple, or even simpler when you can group attributes in common subclasses.
In other cases, it may be necessary to override the key ``._read()`` method, which is the one
that does the work.
In fact, PandasCSV does override ``.discover()``, to add the ``nrows=`` argument,
but adding such refinements is optional.

Doing the above is enough, such that a URL ending in "csv" will be recognised, and pandas offered
as one of the potential readers; and thus we can make a reader instance and store it in a Catalog.

Next, let's imagine we want to make a super simple converter:

.. code-block:: python

    from intake import BaseConverter

    class PandasToStr(BaseConverter):
        instances = {"pandas:DataFrame": "builtins:str"}
        func = "builtins:str"

This just returns the string representation of the dataframe, turning DataFrame instances into
``str`` instances (actually, it would work for just about any python object). The inclusion
of "DataFrame" in ``instances`` means that Intake will know that this is a transform that can be
applied to readers that produce a DataFrame, and it will appear in tab completions and a
reader instance's ``.transform`` attribute.

To complete the pipeline, lets make a outputter which writes this back to a file

.. code-block:: python

    class StrToFile(BaseConverter):
        instances = {"builtins:str": datatypes.Text.qname()}

        def run(self, x, url, storage_options=None, metadata=None, **kwargs):
            with fsspec.open(url, mode="wt", **storage_options) as f:
                f.write(x)
            return datatypes.Text(url=url, storage_options=storage_options, metadata=metadata)

Although we use ``fsspec`` (which is recommended, where possible), the code is again super-simple.
It is conventional, but not necessary, to have such "output" nodes return a datatypes instance.

All of this now allows:

.. code-block:: python

    >>> import intake
    >>> intake.auto_pipeline("blah.csv", "Text")
    PipelineReader:

      0: intake.readers.readers:PandasCSV, () {} => pandas:DataFrame
      1: PandasToStr, () {} => builtins:str
      2: StrToFile, () {} => intake.readers.datatypes:Text

(where the output filename remains to be filled in)

Packaging
---------

Having made a couple of new classes, how would we get these to potential users?

Assuming you are already familiar with how to create a python package _in_general_,
what you need to know, is that Intake will find the new code so long as the classes
are subclasses of BaseData, BaseReader (etc.), and the code is imported. That importing
can be done

- explicitly (which is good form for ad-hoc/experimental use)
- including an `entrypoint`_ for the package in the group "intake.imports",
  where the value would be of the form
  "package.module" or "package:module" (the latter for ``import .. from`` style). This
  requires that the new package is installed via ``pip``, ``conda``, etc.
- adding the package/module to ``intake.conf["extra_imports"]`` and saving; this will take
  effect on the next import of Intake.

.. _entrypoint: https://packaging.python.org/en/latest/specifications/entry-points/

Migration from V1
-----------------

Section :ref:`v1` shows the principal differences to Intake before Take2. From
a developer's viewpoint, if porting former plugins, here are some things to bear in mind.

- in v2 we generally separate out the definition of the data itself versus the
  specific reader, e.g., HDF5 is a file type, but xarray is a reader which can
  handle HDF5. It is totally possible to write a reader without a data type if appropriate.
  See :ref:`base` for an overview of the classes.
- the new readers only really have one method that matters, ``.read()``, and will
  contain all of the previous logic. It should consistently only produce one
  particular output type. Other attributes of BaseReader (or FileReader) are
  one-line overrides and mostly provide information rather than functionality;
  for instance, Intake uses these for recommending readers for a given data instance.
- for catalog-producing readers, the output type will be :class:`intake.readers.entry:Catalog`,
  and the ``.read()`` method will create the Catalog instance and assign readers into it.
  Module ``intake.readers.catalogs`` contains some patterns to copy.
- if using file patterns: the ``DaskCSVPattern`` reader will give an idea of how to
  implement that in the new framework.
- if using V1 plots, dataframe and xarray-producing readers have the ``ToHvPlot``
  converter can be used for similar functionality.


.. raw:: html

    <script data-goatcounter="https://intake.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
