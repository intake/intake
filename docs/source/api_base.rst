Base Classes
------------

This is a reference API class listing, useful mainly for developers.

.. autosummary::
   intake.source.base.DataSourceBase
   intake.source.base.DataSource
   intake.source.base.PatternMixin
   intake.source.base.AliasSource
   intake.container.base.RemoteSource
   intake.catalog.Catalog
   intake.catalog.entry.CatalogEntry
   intake.catalog.local.UserParameter
   intake.auth.base.BaseAuth
   intake.source.cache.BaseCache
   intake.source.base.Schema
   intake.container.persist.PersistStore

.. autoclass:: intake.source.base.DataSource
   :members:

   .. attribute:: plot

      Accessor for HVPlot methods.  See :doc:`plotting` for more details.

.. autoclass:: intake.catalog.Catalog
   :members:

.. autoclass:: intake.catalog.entry.CatalogEntry
   :members:

.. autoclass:: intake.container.base.RemoteSource
   :members:

.. autoclass:: intake.catalog.local.UserParameter
   :members:

.. autoclass:: intake.auth.base.BaseAuth
   :members:

.. autoclass:: intake.source.cache.BaseCache
   :members:

.. autoclass:: intake.source.base.AliasSource
   :members: __init__, _get_source

.. autoclass:: intake.source.base.PatternMixin
   :members:

.. autoclass:: intake.source.base.Schema
   :members:

.. autoclass:: intake.container.persist.PersistStore
   :members: add, get_tok, remove, backtrack, refresh, needs_refresh
