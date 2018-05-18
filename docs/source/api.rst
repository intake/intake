API Reference
=============

.. autoclass:: intake.Catalog

   .. automethod:: intake.Catalog.__init__

   .. automethod:: intake.Catalog.__iter__

   .. automethod:: intake.Catalog.__getitem__

.. autoclass:: intake.catalog.entry.CatalogEntry

   .. automethod:: intake.catalog.entry.CatalogEntry.describe

   .. automethod:: intake.catalog.entry.CatalogEntry.describe_open

   .. automethod:: intake.catalog.entry.CatalogEntry.get

.. autoclass:: intake.source.base.DataSource
   :members: close, discover, read, read_chunked, read_partition, to_dask

   .. attribute:: plot

      Accessor for HoloPlot methods.  See :doc:`plotting` for more details.

.. autoclass:: intake.source.base.Plugin
   :members: open
