from .base import Catalog
from .local import LocalCatalogEntry


class ZarrGroupCatalog(Catalog):
    """A catalog of the members of a Zarr group."""

    version = "0.0.1"
    container = "catalog"
    partition_access = None
    name = "zarr_cat"

    def __init__(self, urlpath, storage_options=None, component=None, metadata=None, consolidated=False, name=None):
        """

        Parameters
        ----------
        urlpath : str
            Location of data file(s), possibly including protocol information
        storage_options : dict, optional
            Passed on to storage backend for remote files
        component : str, optional
            If None, build a catalog from the root group. If given, build the
            catalog from the group at this location in the hierarchy.
        metadata : dict, optional
            Catalog metadata. If not provided, will be populated from Zarr
            group attributes.
        consolidated : bool, optional
            If True, assume Zarr metadata has been consolidated.
        """
        self._urlpath = urlpath
        self._storage_options = storage_options or {}
        self._component = component
        self._consolidated = consolidated
        self._grp = None
        self.name = name
        super().__init__(metadata=metadata)

    def _load(self):
        import zarr

        if self._grp is None:
            # obtain the zarr root group
            if isinstance(self._urlpath, zarr.hierarchy.Group):
                # use already-opened group, allows support for nested groups
                # as catalogs
                root = self._urlpath

            else:
                # obtain store
                if isinstance(self._urlpath, str):
                    # open store from url
                    from fsspec import get_mapper

                    store = get_mapper(self._urlpath, **self._storage_options)
                else:
                    # assume store passed directly
                    store = self._urlpath

                # open root group
                if self._consolidated:
                    # use consolidated metadata
                    root = zarr.open_consolidated(store=store, mode="r")
                else:
                    root = zarr.open_group(store=store, mode="r")

            # deal with component path
            if self._component is None:
                self._grp = root
            else:
                self._grp = root[self._component]

            # use zarr attributes as metadata
            self.metadata.update(self._grp.attrs.asdict())

        # build catalog entries
        entries = {}
        for k, v in self._grp.items():
            if isinstance(v, zarr.core.Array):
                entry = LocalCatalogEntry(name=k, description="", driver="ndzarr", args=dict(urlpath=v), catalog=self)
            else:
                entry = LocalCatalogEntry(name=k, description="", driver="zarr_cat", args=dict(urlpath=v))
            entries[k] = entry
        self._entries = entries

    def to_zarr(self):
        return self._grp
