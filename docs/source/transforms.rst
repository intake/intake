Dataset Transforms
------------------

aka. derived datasets.

.. warning::
    experimental feature, the API may change. The data sources in
    ``intake.source.derived`` are not yet declared as top-level
    named drivers in the package entrypoints.

Intake allows for the definition of data sources which take as their input
another source in the same directory, so that you have the opportunity to
present *processing* to the user of the catalog.

The "target" or a derived data source will normally be a string. In the
simple case, it is the name of a data source in the same catalog. However,
we use the syntax "catalog:source" to refer to sources in other catalogs,
where the part before ":" will be passed to :func:`intake.open_catalog`,
together with any keyword arguments from ``cat_kwargs``.

This can be done by defining classes which inherit from
``intake.source.derived.DerivedSource``, or using one of the pre-defined classes
in the same module, which usually need to be passed a reference to a function
in a python module. We will demonstrate both.

Example
```````
Consider the following *target* dataset, which loads some simple facts
about US states from a CSV file. This  example is taken from the Intake
test suite.

.. code-block::yaml

    sources:
      input_data:
        description: a local data file
        driver: csv
        args:
          urlpath: '{{ CATALOG_DIR }}cache_data/states.csv'

We now show two ways to apply a super-simple transform to this data,
which selects two of the dataframe's columns.


Class Example
~~~~~~~~~~~~~

The first version uses an approach in which the transform is derived in a
data source class, and the parameters passed are specific to the transform type.
Note that the driver is referred to by it's fully-qualified name in the
Intake package.

.. code-block::yaml

      derive_cols:
        driver: intake.source.derived.Columns
        args:
          targets:
            - input_data
          columns: ["state", "slug"]

The source class for this is included in the Intake codebase, but the important
part is:

.. code-block:: python

    class Columns(DataFrameTransform):
        ...

        def pick_columns(self, df):
            return df[self._params["columns"]]

We see that this specific class inherits from ``DataFrameTransform``,
with ``transform=self.pick_columns``. We know
that the inputs and outputs are both dataframes. This allows for some additional validation
and an automated way to infer the output dataframe's schema that reduces the number of line
of code required.

The given method does exactly what you might imagine: it takes and input dataframe and
applies a column selection to it.

Running ``cat.derive_cols.read()`` will indeed, as expected, produce a version of the data
with only the selected columns included. It does this by defining the original dataset,
appying the selection, and then getting Dask to generate the output. For some datastets,
this can mean that the selection is pushed down to the reader, and the data for the dropped
columns is never loaded. The user may choose to do ``.to_dask()`` instead, and manipulate
the lazy dataframe directly, before loading.

Functional Example
~~~~~~~~~~~~~~~~~~

This second version of the same output uses the more generic and flexible
``intake.source.derived.DataFrameTransform``.

.. code-block:: yaml

    derive_cols_func:
      driver: intake.source.derived.DataFrameTransform
      args:
        targets:
          - input_data
        transform: "intake.source.tests.test_derived._pick_columns"
        transform_kwargs:
          columns: ["state", "slug"]

In this case, we pass a reference to a *function* defined in the Intake test suite.
Normally this would be declared in user modules, where perhaps those declarations
and catalog(s) are distributed together as a package.

.. code-block:: python

    def _pick_columns(df, columns):
        return df[columns]

This is, of course, very similar to the method shown in the previous section,
and again applies the selection in the given named argument to the input. Note that
Intake does not support including actual code in your catalog, since we would not
want to allow arbitrary execution of code on catalog load, as opposed to execution.

Loading this data source proceeds exactly the same way as the class-based approach,
above. Both Dask and in-memory (Pandas, via ``.read()``) methods work as expected.
The declaration in YAML, above, is slightly more verbose, but the amount of
code is smaller. This demonstrates a tradeoff between flexibility and concision. If
there were validation code to add for the arguments or input dataset, it would be
less obvious where to put these things.

Barebone Example
~~~~~~~~~~~~~~~~

The previous two examples both did dateframe to dataframe transforms. However, totally
arbitrary computations are possible. Consider the following:

.. code-block:: yaml

  barebones:
    driver: intake.source.derived.GenericTransform
    args:
      targets:
        - input_data
      transform: builtins.len
      transform_kwargs: {}

This applies ``len`` to the input dataframe. ``cat.barebones.describe()`` gives
the output container type as "other", i.e., not specified. The result of ``read()``
on this gives the single number 50, the number of rows in the input data. This class,
and ``DerivedDataSource`` and included with the intent as superclasses, and probably
will not be used directly often.

Execution engine
````````````````

None of the above example specified explicitly where the compute implied by the
transformation will take place. However, most Intake drivers support in-memory containers
and Dask; remembering that the input dataste here is a dataframe. However, the behaviour
is defined in the driver class itself - so it would be fine to write a driver in which
we make different assumptions. Let's suppose, for instance, that the original source
is to be loaded from ``spark`` (see the ``intake-spark`` package), the driver could
explicitly call ``.to_spark`` on the original source, and be assured that it has a
Spark object to work with. It should, of course, explain in its documentation what
assumptions are being made and that, presumably, the user is expected to also call
``.to_spark`` if they wished to directly manipulate the spark object.

API
```

.. autosummary::
   intake.source.derived.DerivedSource
   intake.source.derived.Alias
   intake.source.derived.GenericTransform
   intake.source.derived.DataFrameTransform
   intake.source.derived.Columns

.. autoclass:: intake.source.derived.DerivedSource
   :members: __init__
.. autoclass:: intake.source.derived.Alias
   :members: __init__
.. autoclass:: intake.source.derived.GenericTransform
   :members: __init__
.. autoclass:: intake.source.derived.DataFrameTransform
   :members: __init__
.. autoclass:: intake.source.derived.Columns
   :members: __init__
