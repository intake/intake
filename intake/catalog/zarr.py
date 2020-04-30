from .base import Catalog
from .local import LocalCatalogEntry
from ..source import register_driver


class ZarrCatalog(Catalog):
    """Catalog as described by a Zarr group."""
    version = '0.0.1'
    container = 'catalog'
    partition_access = None
    name = 'zarr_cat'

    def __init__(self, url, consolidated=False, **kwargs):
        self._url = url
        self._consolidated = consolidated
        self._grp = None
        super().__init__(**kwargs)

    def _load(self):
        import zarr

        if self._grp is None:
            # obtain the zarr group to wrap as a catalog
            if isinstance(self._url, zarr.hierarchy.Group):
                # use already-opened group
                self._grp = self._url
            else:
                # open root group from url
                from fsspec import get_mapper
                store = get_mapper(self._url)
                if self._consolidated:
                    # use consolidated metadata
                    self._grp = zarr.open_consolidated(store=store, mode='r')
                else:
                    self._grp = zarr.open_group(store=store, mode='r')

        # build catalog entries
        entries = {}
        for k, v in self._grp.items():
            if isinstance(v, zarr.core.Array):
                entry = LocalCatalogEntry(name=k,
                                          description='',
                                          driver='ndzarr',
                                          args=dict(url=v),
                                          catalog=self)
            else:
                entry = LocalCatalogEntry(name=k,
                                          description='',
                                          driver='zarr_cat',
                                          args=dict(url=v))
            entries[k] = entry
        self._entries = entries

    def to_zarr(self):
        return self._grp


register_driver('zarr_cat', ZarrCatalog)
