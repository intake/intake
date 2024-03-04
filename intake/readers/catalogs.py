"""Data readers which create Catalog objects"""

from __future__ import annotations

import itertools
import json

import fsspec

from intake.readers import datatypes
from intake.readers.entry import Catalog, DataDescription, ReaderDescription
from intake.readers.readers import BaseReader
from intake.readers.utils import LazyDict


class TiledLazyEntries(LazyDict):
    """A dictionary-like, which only loads a key's value from Tiled on demand"""

    def __init__(self, client):
        self.client = client

    def __getitem__(self, item: str) -> ReaderDescription:
        from intake.readers.readers import TiledClient

        client = self.client[item]
        data = datatypes.TiledService(url=client.uri, metadata=client.item)
        if type(client).__name__ == "Node":
            reader = TiledCatalogReader(data=data)
        else:
            reader = TiledClient(
                data,
                output_instance=f"{type(client).__module__}:{type(client).__name__}",
            )
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

    def _read(self, data, **kwargs):
        from tiled.client import from_uri

        opts = data.options.copy()
        opts.update(kwargs)
        client = from_uri(data.url, **opts)
        entries = TiledLazyEntries(client)
        return Catalog(
            entries=entries,
            aliases={k: k for k in sorted(client)},
            metadata=client.item,
        )


class SQLAlchemyCatalog(BaseReader):
    """Uses SQLAlchemy to get the list of tables at some SQL URL

    These tables are presented as data entries in a catalog, but could then be loaded by
    any reader that implements SQLQuery.
    """

    implements = {datatypes.Service}
    imports = {"sqlalchemy"}
    output_instance = "intake.readers.entry:Catalog"

    def _read(self, data, views=True, schema=None, **kwargs):
        import sqlalchemy

        engine = sqlalchemy.create_engine(data.url)
        meta = sqlalchemy.MetaData()
        meta.reflect(bind=engine, views=views, schema=schema)
        tables = list(meta.tables)
        entries = {
            name: DataDescription(
                "intake.readers.datatypes:SQLQuery",
                kwargs={"conn": data.url, "query": name},
            )
            for name in tables
        }
        return Catalog(data=entries)


class StacCatalogReader(BaseReader):
    """Create a Catalog from a STAC endpoint or file

    https://stacspec.org/en
    """

    # STAC organisation: Catalog->Item->Asset. Catalogs can reference Catalogs.
    # also have ItemCollection (from searching a Catalog) and CombinedAsset (multi-layer data)
    # Asset and CombinedAsset are datasets, the rest are Catalogs

    implements = {datatypes.STACJSON, datatypes.Literal}
    imports = {"pystac"}
    output_instance = "intake.readers.entry:Catalog"

    def _read(
        self,
        data,
        cls: str = "Catalog",
        signer: callable | None = None,
        prefer: tuple[str] | str | None = ("xarray", "numpy"),
        **kwargs,
    ):
        """
        Parameters
        ----------
        data:
            JSON-like STAC response, or the actual URL to fetch this from
        cls:
            the class of STAC object this should be, Catalog, Item, Asset, ItemCollection, Feature
        signer:
            if given, apply this function to assets and derived items, to add signature/
            auth information to HTTP calls
        prefer:
            if given, select readers matching this spec from those that might apply to
            a given asset; e.g., "xarray" for those data that can be read into xarray.
        """
        import pystac

        cls = getattr(pystac, cls)
        if isinstance(data, datatypes.JSONFile):
            self._stac = cls.from_file(data.url)
        else:
            self._stac = cls.from_dict(data.data)
        metadata = self._stac.to_dict()
        metadata.pop("links", None)
        self.metadata.update(metadata)
        cat = Catalog(metadata=self.metadata)
        items = []

        # the following can be slow and could be deferred to lazy entries, if we can get
        # the names without details cheaply
        if isinstance(self._stac, pystac.Catalog):
            items = itertools.chain(self._stac.get_children(), self._stac.get_items())
        elif isinstance(self._stac, pystac.ItemCollection):
            items = self._stac.items
        if hasattr(self._stac, "assets"):
            for key, value in self._stac.assets.items():
                if signer:
                    signer(value)
                try:
                    reader = self._get_reader(
                        value, signer=signer, prefer=prefer, metadata=self.metadata
                    ).to_entry()
                    cat[key] = reader
                except (ValueError, TypeError, StopIteration):
                    pass

        for subcatalog in items:
            subcls = type(subcatalog).__name__

            cat[subcatalog.id] = ReaderDescription(
                reader=self.qname(),
                kwargs=dict(
                    {
                        "cls": subcls,
                        "data": datatypes.Literal(subcatalog.to_dict()),
                        "signer": signer,
                        "prefer": prefer,
                    },
                    **kwargs,
                ),
                metadata=self.metadata,
            )
        return cat

    @staticmethod
    def _get_reader(asset, signer=None, prefer=None, metadata=None):
        """
        Assign intake driver for data I/O

        prefer: str | list[str]
            passed to .to_reader to inform what class of reader would be preferable, if any
        """
        url = asset.href
        mime = asset.media_type

        if mime in ["", "null"]:
            mime = None

        # if mimetype not registered try rasterio driver
        storage_options = asset.extra_fields.get("table:storage_options", {})
        if "credential" in storage_options:
            # MS-ABFS specific argument; look for MS identifier?
            storage_options["sas_token"] = storage_options["credential"]
        cls = datatypes.recommend(url, mime=mime, storage_options=storage_options, head=False)
        meta = asset.to_dict()
        if cls:
            data = cls[0](url=url, metadata=meta, storage_options=storage_options)
        else:
            raise ValueError
        if "stac-items" in meta.get("roles", []) or [] and isinstance(data, datatypes.Parquet):
            from intake.readers.readers import DaskGeoParquet

            data.metadata["signer"] = signer
            data.metadata["prefer"] = prefer
            return DaskGeoParquet(data)
        else:
            rr = None
        return data.to_reader(reader=rr, outtype=prefer, metadata=metadata)


class StackBands(BaseReader):
    """Reimplementation of "StackBandsSource" from intake-stac

    Takes the subitems of a given collection and concatenates them using xarray
    on the given dimension.
    """

    implements = {datatypes.STACJSON, datatypes.Literal}
    imports = {"pystac", "xarray"}
    output_instance = "xarray:Dataset"

    def _read(self, data, bands: list[str], concat_dim: str = "band", signer=None, **kw):
        """
        Parameters
        ----------
        data:
            STAC endpoint, file, or dict literal
        bands:
            band_id, name or common_name to select from contained items
        concat_dim:
            concat dimansion in the xarray sense

        Returns
        -------
        Configured reader, so that you can persist it. Call .read() again to execute
        the read and concat operation.
        """
        # this should be a separate reader for STACJSON,
        import pystac
        from pystac.extensions.eo import EOExtension

        cls = pystac.Item
        if isinstance(data, datatypes.JSONFile):
            self._stac = cls.from_file(data.url)
        else:
            self._stac = cls.from_dict(data.data)
        if signer:
            signer(self._stac)

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
                    (
                        b
                        for b in band_info
                        if band in [b.get(_) for _ in ["common_name", "name", "id"]]
                    ),
                    None,
                )
            else:
                info = next(
                    (b for b in band_info if band in [b.get(_) for _ in ["common_name", "name"]]),
                    None,
                )
                if info is not None:
                    band = info.get("id", info.get("name"))

            if band not in assets or info is None:
                valid_band_names = []
                for b in band_info:
                    valid_band_names.append(b.get("id", b.get("name")))
                    valid_band_names.append(b.get("common_name"))
                raise ValueError(
                    f"{band} not found in list of eo:bands in collection."
                    f"Valid values: {sorted(list(set(valid_band_names)))}"
                )
            asset = assets.get(band)
            metadatas[band] = asset.to_dict()
            titles.append(band)
            types.append(asset.media_type)
            hrefs.append(asset.href)

        unique_types = set(types)
        if len(unique_types) != 1:
            raise ValueError(
                f"Stacking failed: bands must have same type, multiple found: {unique_types}"
            )
        reader = StacCatalogReader._get_reader(asset, signer=signer)
        reader.kwargs["dim"] = concat_dim
        reader.kwargs["data"].url = hrefs
        reader.kwargs.update(kw)
        return reader.read()


class StacSearch(BaseReader):
    """
    Get stac objects matching a search spec from a STAC endpoint
    """

    implements = {datatypes.STACJSON}
    imports = {"pystac"}
    output_instance = "intake.readers.entry:Catalog"

    def __init__(self, metadata=None, **kwargs):
        super().__init__(metadata=metadata, **kwargs)

    def _read(self, data, query=None, **kwargs):
        """
        Parameters
        ----------
        query:
            See https://pystac-client.readthedocs.io/en/latest/api.html#item-search
            for a description of the available fields
        kwargs:
            passed to the resulting readers, e.g., for auth information
        """
        import requests
        from pystac import ItemCollection

        req = requests.post(data.url + "/search", json=query)
        out = req.json()
        cat = Catalog(metadata=self.metadata)
        items = ItemCollection.from_dict(out).items
        for subcatalog in items:
            subcls = type(subcatalog).__name__
            cat[subcatalog.id] = ReaderDescription(
                reader=StacCatalogReader.qname(),
                kwargs=dict(
                    **{"cls": subcls, "data": datatypes.Literal(subcatalog.to_dict())},
                    **kwargs,
                ),
            )
        return cat


class STACIndex(BaseReader):
    """Searches stacindex.org for known public STAC data sources"""

    implements = {datatypes.Service}
    output_instance = Catalog.qname()
    imports = {"pystac"}

    def _read(self, *args, **kwargs):
        with fsspec.open("https://stacindex.org/api/catalogs") as f:
            data = json.load(f)
        cat = Catalog()
        for entry in data:
            if entry["isPrivate"]:
                continue
            if entry["isApi"]:
                cat[entry["slug"]] = StacSearch(
                    data=datatypes.STACJSON(entry["url"]),
                    metadata={
                        "title": entry["title"],
                        "description": entry["summary"],
                        "created": entry["created"],
                        "updated": entry["updated"],
                    },
                )
            else:
                cat[entry["slug"]] = StacCatalogReader(
                    data=datatypes.STACJSON(entry["url"]),
                    metadata={
                        "title": entry["title"],
                        "description": entry["summary"],
                        "created": entry["created"],
                        "updated": entry["updated"],
                    },
                )
        return cat


class THREDDSCatalog(Catalog):
    """A catalog provided by a THREDDS service

    This subclass exists, just so we can indicate the possibility of using the server's
    search endpoint and xarray collection
    """


class THREDDSCatalogReader(BaseReader):
    """
    Read from THREDDS endpoint

    https://www.unidata.ucar.edu/software/tds/
    """

    implements = {datatypes.THREDDSCatalog}
    output_instance = THREDDSCatalog.qname()
    imports = {"siphon", "xarray"}

    def _read(self, data, **kwargs):
        """
        Parameters
        ----------
        data:
            Service URL endpoint
        """
        from siphon.catalog import TDSCatalog

        from intake.readers.readers import XArrayDatasetReader

        thr = TDSCatalog(data.url)
        cat = THREDDSCatalog(metadata=thr.metadata)
        for r in thr.catalog_refs.values():
            cat[r.title] = THREDDSCatalogReader(datatypes.THREDDSCatalog(url=r.href))
        for ds in thr.datasets.values():
            cat[ds.id + "_DAP"] = XArrayDatasetReader(
                datatypes.Service(ds.access_urls["OpenDAP"]), engine="pydap"
            )
            cat[ds.id + "_CDF"] = XArrayDatasetReader(
                datatypes.HDF5(ds.access_urls["HTTPServer"]), engine="h5netcdf"
            )

        return cat


class HuggingfaceHubCatalog(BaseReader):
    """
    Datasets from HuggingfaceHub

    To actually access datasets which are not public, you will need to supply
    and appropriate token. By default, you will be able to read any public/example
    data.

    Examples
    --------
    >>> hf = intake.readers.catalogs.HuggingfaceHubCatalog().read()
    >>> hf['acronym_identification'].read()
    DatasetDict({
        train: Dataset({
            features: ['id', 'tokens', 'labels'],
            num_rows: 14006
        })
        validation: Dataset({
            features: ['id', 'tokens', 'labels'],
            num_rows: 1717
        })
        test: Dataset({
            features: ['id', 'tokens', 'labels'],
            num_rows: 1750
        })
    })

    """

    output_instance = "intake.readers.entry:Catalog"
    imports = {"datasets"}
    func = "huggingface_hub:list_datasets"

    def _read(self, *args, with_community_datasets: bool = False, **kwargs):
        """
        Parameters
        ----------
        with_community_datasets:
            If False, only includes official public data, and retrieves fewer entries.
        """
        from intake.readers.datatypes import HuggingfaceDataset
        from intake.readers.readers import HuggingfaceReader
        import huggingface_hub

        datasets = huggingface_hub.list_datasets(full=False)
        if not with_community_datasets:
            datasets = [dataset for dataset in datasets if "/" not in dataset.id]
        entries = [
            HuggingfaceReader(data=HuggingfaceDataset(d.id, metadata=d.__dict__)) for d in datasets
        ]
        cat = Catalog(entries=entries)
        cat.aliases = {d.id: e for d, e in zip(datasets, cat.entries)}
        return cat


class SKLearnExamplesCatalog(BaseReader):
    """
    Example datasets from sklearn.datasets

    https://scikit-learn.org/stable/datasets/toy_dataset.html

    Each entry has some specific parameters, please read the linked page. Note that the metadata
    is only available in the final dataset after .read(), not before.

    Examples
    --------
    >>> import intake.readers.catalogs
    >>> cat = intake.readers.catalogs.SKLearnExamplesCatalog().read()
    >>> list(cat)
    ['breast_cancer',
     'diabetes',
     'digits',
     ...]
    >>> cat.olivetti_faces.read()
    downloading Olivetti faces from https://ndownloader.figshare.com/files/5976027 to TMP
    {'data': array([[0.30991736, 0.3677686 , 0.41735536, ..., 0.15289256, 0.16115703,
             0.1570248 ],
            [0.45454547, 0.47107437, 0.5123967 , ..., 0.15289256, 0.15289256,
             0.15289256],
            [0.3181818 , 0.40082645, 0.49173555, ..., 0.14049587, 0.14876033,
            ...
    """

    output_instance = "intake.readers.entry:Catalog"
    imports = {"sklearn"}

    def _read(self, **kw):
        from intake.readers.readers import SKLearnExampleReader
        import sklearn.datasets

        names = [funcname[5:] for funcname in dir(sklearn.datasets) if funcname.startswith("load_")]
        names.extend(
            [funcname[6:] for funcname in dir(sklearn.datasets) if funcname.startswith("fetch_")]
        )
        entries = [SKLearnExampleReader(name=name) for name in names]
        cat = Catalog(entries=entries)
        cat.aliases = {name: e for name, e in zip(names, cat.entries)}
        return cat


class TorchDatasetsCatalog(BaseReader):
    """
    Standard example PyTorch datasets

    Includes all the entries in packages torchvision, torchaudio, torchtext . The
    types of these data when read are all torch.utils.data:Dataset, but the contents
    are quite different.

    Examples
    --------
    >>> cat = intake.readers.catalogs.TorchDatasetsCatalog(rootdir="here").read()
    >>> cat.RTE.read()
    (ShardingFilterIterDataPipe,
     ShardingFilterIterDataPipe,
     ShardingFilterIterDataPipe)
    """

    # TODO: the load function can be used for a wide variety of local files
    output_instance = "intake.readers.entry:Catalog"
    imports = {"datasets"}

    def _read(self, rootdir: str, *args, **kwargs):
        """
        Parameters
        ----------
        rootdir:
            A local directory to store cached files

        Some datasets require further kwargs, such as subset (e.g., "train") or other
        selector.
        """
        from intake.readers.readers import TorchDataset
        import importlib

        cat = Catalog()
        for name in ("vision", "audio", "text"):
            try:
                mod = importlib.import_module(f"torch{name}")
                for func in mod.datasets.__all__:
                    f = getattr(mod.datasets, func)
                    metadata = (
                        {"description": f.__doc__.split("\n", 1)[0], "text": f.__doc__}
                        if f.__doc__
                        else {}
                    )
                    metadata["section"] = name
                    cat[func] = TorchDataset(
                        modname=name, funcname=func, rootdir=rootdir, metadata=metadata
                    )
            except (ImportError, ModuleNotFoundError):
                pass
        return cat


class TensorFlowDatasetsCatalog(BaseReader):
    """
    Datasets from the TensorFlow public registry

    See https://github.com/tensorflow/datasets/tree/master/tensorflow_datasets for
    full decriptions. Data will be cached locally on load, and metadata is only fetched
    at that time.

    Examples
    --------
    >>> tf = intake.readers.catalogs.TensorFlowDatasetsCatalog().read(
    >>> tf.xnli.read()
    Downloading and preparin ...
    Dataset xnli downloaded and prepared to <>>. Subsequent calls will reuse this data.
    Out[13]:
    ({Split('test'): <PrefetchDataset element_spec={'hypothesis': ...
    """

    output_instance = "intake.readers.entry:Catalog"
    imports = {"tensorflow_datasets"}

    def _read(self, *args, **kwargs):
        from intake.readers.readers import TFPublicDataset
        from tensorflow_datasets.core import community

        cat = Catalog()
        for name in community.registry.registered._DATASET_REGISTRY:
            cat[name] = TFPublicDataset(name=name)
        return cat


class EarthdataReader(BaseReader):
    """Read particular earthdata dataset by ID and parameter bounds

    Requires registration at https://urs.earthdata.nasa.gov/ and calling
    earthaccess.login() before access. For some specific providers, you also
    must allow access within the online portal before loading data.

    Will attempt to read all data
    with xarray as netCDF4/HDF5 files.
    """

    output_instance = "xarray:Dataset"
    imports = {"earthdata", "xarray"}
    func = "earthaccess:search_data"
    func_doc = "earthaccess:open"

    def _read(self, concept, **kwargs):
        import earthaccess
        import xarray as xr

        granules = self._func(concept_id=concept, **kwargs)
        files = earthaccess.open(granules)
        return xr.open_mfdataset(files, engine="h5netcdf")


class EarthdataCatalogReader(BaseReader):
    """Finds the earthdata datasets that contain some data in the given query bounds

    Each entry returns an ``EarthdataReader`` - see further documentation there. The
    keys are the "concept-id" of each dataset, a unique string.

    Examples
    --------

    >>> import intake.readers.catalogs
    >>> cat = intake.readers.catalogs.EarthdataCatalogReader(temporal=("2002-01-01", "2002-01-02")).read()
    >>> reader = cat['C2723754864-GES_DISC']
    >>> reader.read()
    <xarray.Dataset>
    Dimensions:                         (time: 2, lon: 3600, lat: 1800, nv: 2)
    Coordinates:
      * lon                             (lon) float32 -179.9 -179.9 ... 179.9 179.9
      * lat                             (lat) float64 -89.95 -89.85 ... 89.85 89.95
      * time                            (time) datetime64[ns] 2002-01-01 2002-01-02
    Dimensions without coordinates: nv
    Data variables:
        precipitation                   (time, lon, lat) float32 dask.array<chunksize=(1, 3600, 900), meta=np.ndarray>
        precipitation_cnt               (time, lon, lat) int8 dask.array<chunksize=(1, 3600, 900), meta=np.ndarray>
    ...
    """

    output_instance = "intake.readers.entry:Catalog"
    imports = {"earthdata", "xarray"}
    func = "earthaccess:search_datasets"

    def _read(self, temporal=("1980-01-01", "2023-11-10"), **kwargs):
        cat = Catalog()
        dss = self._func(temporal=temporal, cloud_hosted=True, **kwargs)
        for ds in dss:
            cat[ds["meta"]["concept-id"]] = ReaderDescription(
                reader="intake.readers.catalogs:EarthdataReader",
                metadata=dict(ds),
                kwargs=dict(
                    concept=ds["meta"]["concept-id"], temporal=temporal, cloud_hosted=True, **kwargs
                ),
            )
        return cat
