from intake.readers.datatypes import TiledService
from intake.readers.entry import Catalog, ReaderDescription
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
    implements = {TiledService}
    output_instance = {"intake.readers.entry:Catalog"}
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
