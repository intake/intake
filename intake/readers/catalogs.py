from __future__ import annotations

import itertools

from intake.readers import datatypes
from intake.readers.entry import Catalog, DataDescription, ReaderDescription
from intake.readers.readers import BaseReader
from intake.readers.utils import LazyDict


class TiledLazyEntries(LazyDict):
    def __init__(self, client):
        self.client = client

    def __getitem__(self, item: str) -> ReaderDescription:
        from intake.readers.readers import TiledClient

        client = self.client[item]
        data = datatypes.TiledService(url=client.uri, metadata=client.item)
        if type(client).__name__ == "Node":
            reader = TiledCatalogReader(data=data)
        else:
            reader = TiledClient(data, output_instance=f"{type(client).__module__}:{type(client).__name__}")
        return reader.to_entry()

    def __len__(self):
        return len(self.client)

    def __iter__(self):
        return iter(self.client)

    def __repr__(self):
        return f"TiledEntries {sorted(set(self))}"


class TiledCatalogReader(BaseReader):
    """Creates a catalog of Tiled datasets from a root URL

    The generated catalog is lazy, only the list of entries is got eagerly, but they are
    fetched only on demand.
    """

    implements = {datatypes.TiledService}
    output_instance = "intake.readers.entry:Catalog"
    imports = {"tiled"}

    def __init__(self, data, metadata=None, output_instane=None, **kwargs):
        super().__init__(data, metadata=metadata, output_instane=output_instane)

    def read(self, **kwargs):
        from tiled.client import from_uri

        opts = self.data.options.copy()
        opts.update(kwargs)
        client = from_uri(self.data.url, **opts)
        entries = TiledLazyEntries(client)
        return Catalog(entries=entries, aliases={k: k for k in sorted(client)}, metadata=client.item)


class SQLAlchemyCatalog(BaseReader):
    """Uses SQLAlchemy to get the list of tables at some SQL URL

    These tables are presented as data entries in a catalog, but could then be loaded by
    any reader that implements SQLQuery.
    """

    implements = {datatypes.Service}
    imports = {"sqlalchemy"}
    output_instance = "intake.readers.entry:Catalog"

    def __init__(self, data, metadata=None, views=True, schema=None, **kwargs):
        super().__init__(data, metadata)
        self.views = views  # maybe part of the data prescription, which has .options
        self.schema = schema
        self.kwargs = kwargs

    def read(self, **kwargs):
        import sqlalchemy

        engine = sqlalchemy.create_engine(self.data.url)
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=engine, views=self.views, schema=self.schema)
        tables = list(meta.tables)
        entries = {name: DataDescription("intake.readers.datatypes:SQLQuery", kwargs={"conn": self.data.url, "query": name}) for name in tables}
        return Catalog(data=entries)


class StacCatalogReader(BaseReader):
    # STAC organisation: Catalog->Item->Asset. Catalogs can reference Catalogs.
    # also have ItemCollection (from searching a Catalog) and CombinedAsset (multi-layer data)
    # Asset and CombinedAsset are datasets, the rest are Catalogs

    implements = {datatypes.STACJSON, datatypes.Literal}
    imports = {"pystac"}
    output_instance = "intake.readers.entry:Catalog"

    def __init__(self, data, cls: str = "Catalog", metadata=None, **kwargs):
        import pystac

        cls = getattr(pystac, cls)
        super().__init__(data, metadata, **kwargs)
        if isinstance(self.data, datatypes.JSONFile):
            self._stac = cls.from_file(self.data.url)
        else:
            self._stac = cls.from_dict(self.data.data)
        metadata = self._stac.to_dict()
        metadata.pop("links")
        self.metadata.update(metadata)

    def read(self, **kwargs):
        import pystac

        cat = Catalog(metadata=self.metadata)
        items = []
        if isinstance(self._stac, pystac.Catalog):
            items = itertools.chain(self._stac.get_children(), self._stac.get_items())
        elif isinstance(self._stac, pystac.ItemCollection):
            items = self._stac.items
        elif isinstance(self._stac, pystac.Item):
            # these make assets, which are not catalogs
            for key, value in self._stac.assets.items():
                cat[key] = self._get_reader(value).to_entry()

        for subcatalog in items:
            subcls = type(subcatalog).__name__

            # TODO: items may also be readable by stack_bands
            cat[subcatalog.id] = ReaderDescription(
                data=datatypes.Literal(subcatalog.to_dict()).to_entry(),
                reader=self.qname(),
                kwargs={"cls": subcls},
            )
        return cat

    def stack_bands(self, bands, concat_dim="band"):
        # this should be a separate reader for STACJSON,
        from pystac.extensions.eo import EOExtension

        band_info = [band.to_dict() for band in EOExtension.ext(self._stac).bands]
        metadatas = {}
        titles = []
        hrefs = []
        types = []
        assets = self._stac.assets
        for band in bands:
            # band can be band id, name or common_name
            if band in assets:
                info = next(
                    (b for b in band_info if b.get("id", b.get("name")) == band),
                    None,
                )
            else:
                info = next((b for b in band_info if b.get("common_name") == band), None)
                if info is not None:
                    band = info.get("id", info.get("name"))

            if band not in assets or info is None:
                valid_band_names = []
                for b in band_info:
                    valid_band_names.append(b.get("id", b.get("name")))
                    valid_band_names.append(b.get("common_name"))
                raise ValueError(f"{band} not found in list of eo:bands in collection." f"Valid values: {sorted(list(set(valid_band_names)))}")
            asset = assets.get(band)
            metadatas[band] = asset.to_dict()
            titles.append(band)
            types.append(asset.media_type)
            hrefs.append(asset.href)

        unique_types = set(types)
        if len(unique_types) != 1:
            raise ValueError(f"Stacking failed: bands must have same type, multiple found: {unique_types}")
        reader = StacCatalogReader._get_reader(asset)
        reader.kwargs["concat_dim"] = concat_dim
        reader.metadata.update(metadatas)
        reader.metadata["description"] = ", ".join(titles)
        reader.data.url = hrefs
        return reader

    @staticmethod
    def _get_reader(asset):
        """
        Assign intake driver for data I/O
        """
        url = asset.href
        mime = asset.media_type

        if mime in ["", "null"]:
            mime = None

        # if mimetype not registered try rasterio driver
        storage_options = asset.extra_fields.get("xarray:storage_options", {})
        cls = datatypes.recommend(url, mime=mime, storage_options=storage_options)
        meta = asset.to_dict()
        if cls:
            data = cls[0](url=url, metadata=meta, storage_options=storage_options)
        else:
            raise ValueError
        try:
            return data.to_reader(outtype="xarray:DataSet")
        except ValueError:
            # no xarray reader
            if data.possible_readers:
                return data.to_reader()
            else:
                return data


class StacSearch(BaseReader):
    implements = {datatypes.STACJSON}
    imports = {"pystac"}
    output_instance = "intake.readers.entry:Catalog"

    def __init__(self, data, query, metadata=None, **kwargs):
        self.query = query
        super().__init__(data, metadata=metadata)

    def read(self, query=None, **kwargs):
        import requests
        from pystac import ItemCollection

        query = query or self.query
        req = requests.post(self.data.url + "/search", json=query)
        out = req.json()
        cat = Catalog(metadata=self.metadata)
        items = ItemCollection.from_dict(out).items
        for subcatalog in items:
            subcls = type(subcatalog).__name__
            cat[subcatalog.id] = ReaderDescription(
                data=datatypes.Literal(subcatalog.to_dict()).to_entry(),
                reader=StacCatalogReader.qname(),
                kwargs={"cls": subcls},
            )
        return cat


class THREDDSCatalog(Catalog):
    ...


class THREDDSCatalogReader(BaseReader):
    implements = {datatypes.THREDDSCatalog}
    output_instance = THREDDSCatalog.qname()
    imports = {"siphon", "xarray"}

    def __init__(self, data, **kwargs):
        super().__init__(data, **kwargs)

    def read(self, **kwargs):
        from siphon.catalog import TDSCatalog

        from intake.readers.readers import XArrayDatasetReader

        thr = TDSCatalog(self.data.url)
        cat = Catalog(metadata=thr.metadata)
        for r in thr.catalog_refs.values():
            cat[r.title] = THREDDSCatalogReader(datatypes.THREDDSCatalog(url=r.href))
        for ds in thr.datasets.values():
            cat[ds.id + "_DAP"] = XArrayDatasetReader(datatypes.Service(ds.access_urls["OpenDAP"]), engine="pydap")
            cat[ds.id + "_CDF"] = XArrayDatasetReader(datatypes.HDF5(ds.access_urls["HTTPServer"]), engine="h5netcdf")

        return cat
