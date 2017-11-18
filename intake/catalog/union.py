from .local import CatalogBase


class UnionCatalog(CatalogBase):
    collections = 0

    def __init__(self, catalogs, name=None):
        super(UnionCatalog, self).__init__(catalogs, name=name)
        # No plugins intrinsic to this catalog
        self.source_plugins = {}
        self._entries = {c.name: c for c in catalogs}
        if name:
            self.name = name
        else:
            self.name = "Union%i" % self.collections
            self.collections += 1

    def __repr__(self):
        return "<Set of catalogs: %s>" % set(self._entries)
