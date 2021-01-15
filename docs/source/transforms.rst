Dataset Transforms
------------------

aka. derived datasets.

(experimental)

Intake allows for the definition of data sources which take as their input
another source in the same directory, so that you have the opportunity to
present *processing* to the user of the catalog.

Example
~~~~~~~

This example is taken from the Intake test suite.

Text to come, watch this space...

API
~~~

.. autosummary::
   intake.source.derived.DerivedSource
   intake.source.derived.Alias
   intake.source.derived.GenericTransform
   intake.source.derived.DataFrameTransform
   intake.source.derived.Columns

.. autoclass:: intake.source.derived.DerivedSource
   :members:
.. autoclass:: intake.source.derived.Alias
   :members:
.. autoclass:: intake.source.derived.GenericTransform
   :members:
.. autoclass:: ntake.source.derived.DataFrameTransform
   :members:
.. autoclass:: intake.source.derived.Columns
   :members:
