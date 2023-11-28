End User
--------

These are reference class and function definitions likely to be useful to everyone.

.. autosummary::
   intake.open_catalog
   intake.registry
   intake.register_driver
   intake.unregister_driver
   intake.source.csv.CSVSource
   intake.source.textfiles.TextFilesSource
   intake.source.jsonfiles.JSONFileSource
   intake.source.jsonfiles.JSONLinesFileSource
   intake.source.npy.NPySource
   intake.source.zarr.ZarrArraySource
   intake.catalog.local.YAMLFileCatalog
   intake.catalog.local.YAMLFilesCatalog
   intake.catalog.zarr.ZarrGroupCatalog
   intake.interface.gui.GUI

.. autofunction::
   intake.open_catalog

.. attribute:: intake.registry

   Mapping from plugin names to the DataSource classes that implement them. These are the
   names that should appear in the ``driver:`` key of each source definition in a
   catalog. See :doc:`plugin-directory` for more details.

.. attribute:: intake.open_

   Set of functions, one for each plugin, for direct opening of a data source. The names are derived from the names of
   the plugins in the registry at import time.

.. autoclass:: intake.interface.gui.GUI
   :members:

Source classes
''''''''''''''

.. autoclass:: intake.source.csv.CSVSource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.source.zarr.ZarrArraySource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.source.textfiles.TextFilesSource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.source.jsonfiles.JSONFileSource
   :members: __init__, discover, read

.. autoclass:: intake.source.jsonfiles.JSONLinesFileSource
   :members: __init__, discover, read, head

.. autoclass:: intake.source.npy.NPySource
   :members: __init__, discover, read_partition, read, to_dask

.. autoclass:: intake.catalog.local.YAMLFileCatalog
   :members: __init__, reload, search, walk

.. autoclass:: intake.catalog.local.YAMLFilesCatalog
   :members: __init__, reload, search, walk

.. autoclass:: intake.catalog.zarr.ZarrGroupCatalog
   :members: __init__, reload, search, walk, to_zarr

.. raw:: html

    <script data-goatcounter="https://intake.goatcounter.com/count"
        async src="//gc.zgo.at/count.js"></script>
