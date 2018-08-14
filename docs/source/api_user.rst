API Reference: User Methods
===========================

.. autosummary::
   intake.open_catalog
   intake.registry
   intake.open_
   intake.source.csv.CSVSource
   intake.source.textfiles.TextFilesSource
   intake.gui.DataBrowser

.. autofunction::
   intake.open_catalog

.. attribute:: intake.registry

   Mapping from plugin names to the DataSource classes that implement them. These are the
   names that should appear in the ``driver:`` key of each source definition in a
   catalog. See :doc:`plugin-directory` for more details.

.. attribute:: intake.open_

   Set of functions, one for each plugin, for direct opening of a data source. The names are derived from the names of
   the plugins in the registry at import time.

.. autoclass:: intake.source.csv.CSVSource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.source.textfiles.TextFilesSource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.source.npy.NPySource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.gui.DataBrowser
   :members:
