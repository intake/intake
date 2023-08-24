from intake.readers.datatypes import Service, TiledService
from intake.readers.entry import Catalog, DataDescription, ReaderDescription
from intake.readers.readers import BaseReader
from intake.readers.utils import LazyDict


class TiledLazyEntries(LazyDict):
    def __init__(self, client):
        self.client = client

    def __getitem__(self, item: str) -> ReaderDescription:
        from intake.readers.readers import TiledClient

        client = self.client[item]
        data = TiledService(url=client.uri, metadata=client.item)
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

    implements = {TiledService}
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

    implements = {Service}
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
