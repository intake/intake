from tiled.client import from_uri
from tiled.client.node import Node

from intake.catalog import Catalog
from intake.source import DataSource


class TiledCatalog(Catalog):
    """View Tiled server as a catalog

    See the documentation for setting up such a server at
    https://blueskyproject.io/tiled/

    A tiled server may contain sources of dataframe, array or xarray type.
    This driver exposes the full tree as exposed by the server, but you
    can also specify the sub-path of that tree.
    """

    name = "tiled_cat"

    def __init__(self, server, path=None):
        """

        Parameters
        ----------
        server: str or tiled.client.node.Node
            Location of tiles server. Usually of the form "http[s]://address:port/"
            May include a path. If the protocol is "tiled", we assume HTTP
            connection. Alternatively, can be a Node instance, already connected
            to a server.
        path: str (optional)
            If given, restrict the catalog to this part of the server's catalog
            tree. Equivalent to extending the server URL.
        """
        self.path = path
        if isinstance(server, str):
            if server.startswith("tiled"):
                uri = server.replace("tiled", "http", 1)
            else:
                uri = server
            client = from_uri(uri, "dask")
        else:
            client = server
            uri = server.uri
        self.uri = uri
        if path is not None:
            client = client[path]
        super().__init__(entries=client, name="tiled:" + uri.split(":", 1)[1])

    def search(self, query, type="text"):
        """Full text search

        Queries other than full text will be added later
        """
        if type == "text":
            from tiled.queries import FullText

            q = FullText(query)
        else:
            raise NotImplementedError
        return TiledCatalog.from_dict(self._entries.search(q), uri=self.uri, path=self.path)

    def __getitem__(self, item):
        node = self._entries[item]
        if isinstance(node, Node):
            return TiledCatalog(node)
        else:
            return TiledSource(uri=self.uri, path=item, instance=node)


types = {
    "DaskArrayClient": "ndarray",
    "DaskDataArrayClient": "xarray",
    "DaskDatasetClient": "xarray",
    "DaskVariableClient": "xarray",
    "DaskDataFrameClient": "dataframe",
}


class TiledSource(DataSource):
    """A source on a Tiled server

    The container type of this source is determined at runtime.
    The attribute ``.instance`` gives access to the underlying Tiled
    API, but most users will only call ``.to_dask()``.
    """

    name = "tiled"

    def __init__(self, uri="", path="", instance=None, metadata=None):
        """

        Parameters
        ----------
        uri: str (optional)
            Location of the server. If ``instance`` is given, this is
            only used for the repr
        pathL str (optional)
            Path of the data source within the server tree. If ``instance``
            is given, this is only used for the repr
        instance: tiled.client.node.None (optional)
            The tiled object pointing to the data source; normally created
            by a ``TiledCatalog``
        metadata: dict
            Extra metadata for this source; metadata will also be provided
            by the server.
        """
        if instance is None:
            instance = from_uri(uri, "dask")[path].read()
        self.instance = instance
        md = dict(instance.metadata)
        if metadata:
            md.update(metadata)
        super().__init__(metadata=md)
        self.name = path
        self.container = types[type(self.instance).__name__]

    def discover(self):
        x = self.to_dask()
        dt = getattr(x, "dtype", None) or getattr(x, "dtypes", None)
        parts = getattr(x, "npartitions", None) or x.data.npartitions
        return dict(dtype=dt, shape=getattr(self.instance.structure().macro, "shape", x.shape), npartitions=parts, metadata=self.metadata)

    def to_dask(self):
        # cache this?
        return self.instance.read()

    def read(self):
        return self.instance.read().compute()

    def _yaml(self):
        y = super()._yaml()
        v = list(y["sources"].values())[0]
        v["args"].pop("instance")
        return y
