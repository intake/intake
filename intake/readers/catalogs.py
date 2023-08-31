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


class StacCatalog(BaseReader):
    # STAC organisation: Catalog->Item->Asset. Catalogs can reference Catalogs.
    # also have ItemCollection (from searching a Catalog) and CombinedAsset (multi-layer data)
    # Asset and CombinedAsset are datasets, the rest are Catalogs

    implements = {datatypes.JSONFile, datatypes.Literal}
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

            cat[subcatalog.id] = ReaderDescription(
                data=datatypes.Literal(subcatalog.to_dict()).to_entry(),
                reader=self.qname(),
                kwargs={"cls": subcls},
            )
        return cat

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
        if cls:
            data = cls[0](url=url, storage_options=storage_options)
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
