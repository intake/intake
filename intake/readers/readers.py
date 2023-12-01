"""Classes for reading data into a python object"""

from __future__ import annotations

import inspect
import itertools
import re

import fsspec

from intake import import_name, logger
from intake.readers import datatypes
from intake.readers.mixins import PipelineMixin
from intake.readers.utils import Tokenizable, subclasses


class BaseReader(Tokenizable, PipelineMixin):
    imports: set[str] = set()  # allow for pip-style versions maybe
    implements: set[datatypes.BaseData] = set()
    optional_imports: set[str] = set()
    func: str = "builtins:NotImplementedError"  # usual function for loading data
    func_doc: str = None  # docstring origin if not from func
    output_instance: str = None
    other_funcs: set = set()  # function names to recognise

    def __init__(
        self,
        *args,
        metadata: dict | None = None,
        output_instance: str | None = None,
        **kwargs,
    ):
        self.kwargs = kwargs
        if args:
            self.kwargs["args"] = args
        met = {}
        for a in itertools.chain(
            reversed(kwargs.get("args", [])), reversed(kwargs.values()), reversed(args)
        ):
            if isinstance(a, datatypes.BaseData):
                met.update(a.metadata)
        met.update(metadata or {})
        self.metadata = met
        if output_instance:
            self.output_instance = output_instance

    def __repr__(self):
        return f"{type(self).__name__} reader producing {self.output_instance}"

    def __call__(self, *args, **kwargs):
        """New version of this instance with altered arguments"""
        kw = self.kwargs.copy()
        kw.update(kwargs)
        if args:
            kw["args"] = args
        return type(self)(**kw)

    @classmethod
    def doc(cls):
        """Doc associated with loading function"""
        f = cls.func_doc or cls.func
        if isinstance(f, str):
            f = import_name(f)
        upstream = f.__doc__ if f is not NotImplementedError else ""
        sig = str(inspect.signature(cls._read))
        doc = cls._read.__doc__
        return "\n\n".join(_ for _ in [cls.qname(), cls.__doc__, sig, doc, upstream] if _)

    def discover(self, **kwargs):
        """Part of the data

        The intent is to return a minimal dataset, but for some readers and conditions this may be
        up to the whole of the data. Output type is the same as for read().
        """
        return self.read(**kwargs)

    @property
    def _func(self):
        """Import and replace .func, if it is a string"""
        if isinstance(self.func, str):
            return import_name(self.func)
        return self.func

    def read(self, *args, **kwargs):
        """Produce data artefact

        Any of the arguments encoded in the data instance can be overridden.

        Output type is given by the .output_instance attribute
        """
        logger.debug("Reading %s", self)
        kw = self.kwargs.copy()
        kw.update(kwargs)
        args = kw.pop("args", ()) or args
        return self._read(*args, **kw)

    def _read(self, *args, **kwargs):
        """This is the method subclasses will tend to override"""
        raise NotImplementedError

    def to_entry(self):
        """Create an entry version of this, ready to be inserted into a Catalog"""
        from intake.readers.entry import ReaderDescription

        return ReaderDescription(
            reader=self.qname(),
            kwargs=self.kwargs,
            output_instance=self.output_instance,
            metadata=self.metadata,
        )

    def to_cat(self, name=None):
        """Create a Catalog containing on this reader"""
        return self.to_entry().to_cat(name)


class FileReader(BaseReader):
    """Convenience superclass for readers of files"""

    url_arg = "url"
    other_urls = {}  # if we have other_funcs, may have different url_args for each
    storage_options = False

    def _read(self, data, **kw):
        kw[self.url_arg] = data.url
        if self.storage_options and data.storage_options:
            kw["storage_options"] = data.storage_options
        return self._func(**kw)


class FileByteReader(FileReader):
    """The contents of file(s) as bytes objects"""

    output_instance = "builtin:bytes"
    implements = {datatypes.FileData}

    def discover(self, data=None, **kwargs):
        data = data or self.kwargs["data"]
        with fsspec.open(data.url, mode="rb", **(data.storage_options or {})) as f:
            return f.read()

    def _read(self, data, **kwargs):
        out = []
        for of in fsspec.open_files(data.url, mode="rb", **(data.storage_options or {})):
            with of as f:
                out.append(f.read())
        return b"".join(out)


class Pandas(FileReader):
    imports = {"pandas"}
    output_instance = "pandas:DataFrame"
    storage_options = True


class PandasParquet(Pandas):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "pandas:read_parquet"
    url_arg = "path"


class PandasFeather(Pandas):
    implements = {datatypes.Feather2, datatypes.Feather1}
    imports = {"pandas", "pyarrow"}
    func = "pandas:read_feather"
    url_arg = "path"


class PandasORC(Pandas):
    implements = {datatypes.ORC}
    imports = {"pandas", "pyarrow"}
    func = "pandas:read_orc"
    url_arg = "path"


class PandasExcel(Pandas):
    implements = {datatypes.Excel}
    imports = {"pandas", "openpyxl"}
    func = "pandas:read_excel"
    url_arg = "io"


class PandasSQLAlchemy(BaseReader):
    implements = {datatypes.SQLQuery}
    func = "pandas:read_sql"
    imports = {"sqlalchemy", "pandas"}
    output_instance = "pandas:DataFrame"

    def discover(self, **kwargs):
        if "chunksize" not in kwargs:
            kwargs["chunksize"] = 10
        return next(iter(self.read(**kwargs)))

    def _read(self, data, **kwargs):
        read_sql = import_name(self.func)
        return read_sql(sql=data.query, con=data.conn, **kwargs)


class DaskDF(FileReader):
    imports = {"dask", "pandas"}
    output_instance = "dask.dataframe:DataFrame"
    storage_options = True


class DaskParquet(DaskDF):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "dask.dataframe:read_parquet"
    url_arg = "path"


class DaskHDF(DaskDF):
    implements = {datatypes.HDF5}
    optional_imports = {"h5py"}
    func = "dask.dataframe:read_hdf"
    url_arg = "pattern"

    def _read(self, data, **kw):
        return self._func(data.url, key=data.path, **kw)


class DaskJSON(DaskDF):
    implements = {datatypes.JSONFile}
    func = "dask.dataframe:read_json"
    url_arg = "url_path"


class DaskSQL(BaseReader):
    implements = {datatypes.SQLQuery}
    imports = {"dask", "pandas", "sqlalchemy"}
    func = "dask.dataframe:read_sql"

    def _read(self, data, index_col, **kw):
        """Dask requires `index_col` to partition the dataframe on."""
        return self._func(data.quary, data.conn, index_col, **kw)


class DaskNPYStack(FileReader):
    """Requires a directory with .npy files and an "info" pickle file"""

    # TODO: single npy file, or stack without info (which can be read from any one file)
    implements = {datatypes.NumpyFile}
    imports = {"dask", "numpy"}
    func = "dask.array:from_npy_stack"
    output_instance = "dask.array:Array"
    url_arg = "dirname"


class DaskZarr(FileReader):
    implements = {datatypes.Zarr}
    imports = {"dask", "zarr"}
    output_instance = "dask.array:Array"
    func = "dask.array:from_zarr"

    def _read(self, data, **kwargs):
        return self._func(
            url=data.url,
            component=data.root or None,
            storage_options=data.storage_options,
            **kwargs,
        )


class NumpyZarr(FileReader):
    implements = {datatypes.Zarr}
    imports = {"zarr"}
    output_instance = "numpy:ndarray"
    func = "zarr:open"

    def _read(self, data, **kwargs):
        return self._func(data.url, storage_options=data.storage_options, path=data.root, **kwargs)[
            :
        ]


class DuckDB(BaseReader):
    imports = {"duckdb"}
    output_instance = "duckdb:DuckDBPyRelation"  # can be converted to pandas with .df
    func_doc = "duckdb:query"
    _dd = {}  # hold the engines, so results are still valid

    def discover(self, **kwargs):
        return self.read().limit(10)

    def _duck(self, data):
        import duckdb

        conn = getattr(data, "conn", {})  # only SQL type normally has this
        if isinstance(conn, str):
            # https://duckdb.org/docs/extensions/
            if conn.startswith("sqlite:"):
                duckdb.connect(":default:").execute("INSTALL sqlite;LOAD sqlite;")
                conn = re.sub("^sqlite3?://", "", conn)
                conn = {"database": conn}
            elif conn.startswith("postgres"):
                d = duckdb.connect()
                d.execute("INSTALL postgres;LOAD postgres;")
                # extra params possible here https://duckdb.org/docs/extensions/postgres_scanner#usage
                d.execute(f"CALL postgres_attach('{conn}');")
                self._dd[str(conn)] = d
        if str(conn) not in self._dd:
            self._dd[str(conn)] = duckdb.connect(
                **conn
            )  # connection must be cached for results to be usable
        return self._dd[str(conn)]


class DuckParquet(DuckDB, FileReader):
    implements = {datatypes.Parquet}

    def _read(self, data, **kwargs):
        return self._duck(data).query(f"SELECT * FROM read_parquet('{data.url}')")


class DuckCSV(DuckDB, FileReader):
    implements = {datatypes.CSV}

    def _read(self, data, **kwargs):
        return self._duck(data).query(f"SELECT * FROM read_csv_auto('{data.url}')")


class DuckJSON(DuckDB, FileReader):
    implements = {datatypes.JSONFile}

    def _read(self, data, **kwargs):
        return self._duck(data).query(f"SELECT * FROM read_json_auto('{data.url}')")


class DuckSQL(DuckDB):
    implements = {datatypes.SQLQuery}

    def _read(self, data, **kwargs):
        words = len(data.query.split())
        q = data.query if words > 1 else f"SELECT * FROM {data.query}"
        return self._duck(data).query(q)


class SparkDataFrame(FileReader):
    imports = {"pyspark"}
    func_doc = "pyspark.sql:SparkSession.read"
    output_instance = "pyspark.sql:DataFrame"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)

    def _spark(self):
        SparkSession = import_name("pyspark.sq:SparkSession")
        return SparkSession.builder.getOrCreate()


class SparkCSV(SparkDataFrame):
    implements = {datatypes.CSV}

    def _read(self, data, **kwargs):
        return self._spark().read.csv(data.url, **kwargs)


class SparkParquet(SparkDataFrame):
    implements = {datatypes.Parquet}

    def _read(self, data, **kwargs):
        return self._spark().read.parquet(data.url, **kwargs)


class SparkText(SparkDataFrame):
    implements = {datatypes.Text}

    def _read(self, data, **kwargs):
        return self._spark().read.text(data.url, **kwargs)


class HuggingfaceReader(BaseReader):
    imports = {"datasets"}
    implements = {datatypes.HuggingfaceDataset}
    func = "datasets:load_dataset"
    output_instance = "datasets.arrow_dataset:Dataset"

    def _read(self, data, *args, **kwargs):
        return self._func(data.name, split=data.split, **kwargs)


class SKLearnExampleReader(BaseReader):
    func = "sklearn:datasets"
    imports = {"sklearn"}
    output_instance = "sklearn.utils:Bunch"

    def _read(self, name, **kw):
        import sklearn.datasets

        loader = getattr(sklearn.datasets, f"load_{name}", None) or getattr(
            sklearn.datasets, f"fetch_{name}"
        )
        return loader()


class TorchDataset(BaseReader):
    output_instance = "torch.utils.data:Dataset"

    def _read(self, modname, funcname, rootdir, **kw):
        import importlib

        mod = importlib.import_module(f"torch{modname}")
        func = getattr(mod.datasets, funcname)
        try:
            return func(rootdir, download=True)
        except TypeError:
            return func(rootdir)


class TFPublicDataset(BaseReader):
    # contains ({split: tensorflow.data.Dataset}, data_info) by default
    output_instance = "builtins:tuple"
    func = "tensorflow_datasets:load"

    def _read(self, name, *args, **kwargs):
        return self._func(name, download=True, with_info=True, **kwargs)


class TFTextreader(FileReader):
    imports = {"tensorflow"}
    implements = {datatypes.Text}
    func = "tensorflow.data:TextLineDataset"
    output_instance = "tensorflow.data:Dataset"
    url_arg = "filenames"


class TFORC(FileReader):
    imports = {"tensorflow_io"}
    implements = {datatypes.ORC}
    func = "tensorflow_io:IODataset.from_orc"
    url_arg = "filename"
    output_instance = "tensorflow.data:Dataset"


class TFSQL(BaseReader):
    imports = {"tensorflow_io"}
    implements = {datatypes.SQLQuery}
    func = "tensorflow_io:experimental.IODataset.from_sql"
    output_instance = "tensorflow.data:Dataset"

    def _read(self, data, **kwargs):
        return self._func(endpoint=data.conn, query=data.query, **kwargs)


class KerasImageReader(FileReader):
    imports = {"keras"}
    implements = {datatypes.PNG, datatypes.JPEG}  # others
    func = "keras.utils:image_dataset_from_directory"
    output_instance = "tensorflow.data:Dataset"
    url_arg = "directory"


class KerasText(FileReader):
    imports = {"keras"}
    implements = {datatypes.Text}
    func = "keras.utils:text_dataset_from_directory"
    output_instance = "tensorflow.data:Dataset"
    url_arg = "directory"


class KerasAudio(FileReader):
    imports = {"keras"}
    implements = {datatypes.WAV}
    func = "keras.utils:audio_dataset_from_directory"
    output_instance = "tensorflow.data:Dataset"
    url_arg = "directory"


class KerasModelReader(FileReader):
    imports = {"keras"}
    implements = {datatypes.KerasModel}
    func = "tensorflow.keras.models:load_model"
    url_arg = "filepath"
    output_instance = "keras.engine.training:Model"


class TFRecordReader(FileReader):
    imports = {"tensorflow"}
    implements = {datatypes.TFRecord}
    func = "tensorflow.data:TFRecordDataset"
    output_instance = "tensorflow.data:TFRecordDataset"
    url_arg = "filenames"


class SKLearnModelReader(FileReader):
    # https://scikit-learn.org/stable/model_persistence.html
    # recommends skops, which seems little used
    imports = {"sklearn"}
    implements = {datatypes.SKLearnPickleModel}
    func = "pickle:load"
    output_instance = "sklearn.base:BaseEstimator"

    def _read(self, data, **kw):
        with fsspec.open(data.url, **(data.storage_options or {})) as f:
            return self._func(f)


class Awkward(FileReader):
    imports = {"awkward"}
    output_instance = "awkward:Array"
    storage_options = True


class AwkwardParquet(Awkward):
    implements = {datatypes.Parquet}
    imports = {"awkward", "pyarrow"}
    func = "awkward:from_parquet"
    url_arg = "path"

    def discover(self, **kwargs):
        kwargs["row_groups"] = [0]
        return self.read(**kwargs)


class DaskAwkwardParquet(AwkwardParquet):
    imports = {"dask_awkward", "pyarrow", "dask"}
    func = "dask_awkward:from_parquet"
    output_instance = "dask_awkward:Array"

    def discover(self, **kwargs):
        return self.read(**kwargs).partitions[0]


class AwkwardJSON(Awkward):
    implements = {datatypes.JSONFile}
    func = "awkward:from_json"
    url_arg = "source"


class AwkwardAVRO(Awkward):
    implements = {datatypes.AVRO}
    func = "awkward:from_avro_file"
    url_arg = "file"


class DaskAwkwardJSON(Awkward):
    imports = {"dask_awkward", "dask"}
    func = "dask_awkward:from_json"
    output_instance = "dask_awkward:Array"
    url_arg = "source"

    def discover(self, **kwargs):
        return self.read(**kwargs).partitions[0]


class PandasCSV(Pandas):
    implements = {datatypes.CSV}
    func = "pandas:read_csv"
    url_arg = "filepath_or_buffer"

    def discover(self, **kw):
        kw["nrows"] = 10
        kw.pop("skipfooter", None)
        kw.pop("chuknsize", None)
        return self.read(**kw)


class PandasHDF5(Pandas):
    implements = {datatypes.HDF5}
    func = "pandas:read_hdf"
    imports = {"pandas", "pytables"}

    def _read(self, data, **kw):
        if data.storage_options:  # or fsspec-like
            with fsspec.open(data.url, "rb", **data.storage_options) as f:
                self._func(f, data.path, **kw)
        return self._func(data.url, **kw)


class DaskCSV(DaskDF):
    implements = {datatypes.CSV}
    func = "dask.dataframe:read_csv"
    url_arg = "urlpath"


class Ray(FileReader):
    # https://docs.ray.io/en/latest/data/creating-datasets.html#supported-file-formats
    imports = {"ray"}
    output_instance = "ray.data:Dataset"
    url_arg = "paths"

    def discover(self, **kwargs):
        return self.read(**kwargs).limit(10)


class RayParquet(Ray):
    implements = {datatypes.Parquet}
    func = "ray.data:read_parquet"


class RayCSV(Ray):
    implements = {datatypes.CSV}
    func = "ray.data:read_csv"


class RayJSON(Ray):
    implements = {datatypes.JSONFile}
    func = "ray.data:read_json"


class RayText(Ray):
    implements = {datatypes.Text}
    func = "ray.data:read_text"


class DeltaReader(FileReader):
    implements = {datatypes.Parquet}
    imports = {"deltalake"}
    func = "deltalake:DeltaTable"
    url_arg = "table_uri"
    storage_options = True
    output_instance = "deltalake:DeltaTable"


class TiledNode(BaseReader):
    implements = {datatypes.TiledService}
    imports = {"tiled"}
    output_instance = "tiled.client.node:Node"
    func = "tiled.client:from_uri"

    def _read(self, data, **kwargs):
        opts = data.options.copy()
        opts.update(kwargs)
        return self._func(data.url, **opts)


class TiledClient(BaseReader):
    # returns dask/normal x/array/dataframe
    implements = {datatypes.TiledDataset}
    output_instance = "tiled.client.base:BaseClient"

    def _read(self, data, as_client=True, dask=False, **kwargs):
        from tiled.client import from_uri

        opts = data.options.copy()
        opts.update(kwargs)
        if dask:
            opts["structure_clients"] = "dask"
        client = from_uri(data.url, **opts)
        if as_client:
            return client
        else:
            return client.read()


class PythonModule(BaseReader):
    output_instance = "builtins:module"
    implements = {datatypes.PythonSourceCode}

    def _read(self, data, module_name=None, **kwargs):
        from types import ModuleType

        if module_name is None:
            module_name = data.url.rsplit("/", 1)[-1].split(".", 1)[0]
        with fsspec.open(data.url, "rt", **(data.storage_options or {})) as f:
            mod = ModuleType(module_name)
            exec(f.read(), mod.__dict__)
            return mod


class SKImageReader(FileReader):
    output_instance = "numpy:ndarray"
    imports = {"scikit-image"}
    implements = {datatypes.PNG, datatypes.TIFF, datatypes.JPEG}
    func = "skimage.io:imread"
    url_arg = "fname"


class NumpyText(FileReader):
    output_instance = "numpy:ndarray"
    implements = {datatypes.Text}
    imports = {"numpy"}
    func = "numpy:loadtxt"

    def _read(self, data, **kw):
        if data.storage_options or "://" in data.url or "::" in data.url:
            with fsspec.open(data.url, **(data.storage_options or {})) as f:
                return self._func(f, **kw)
        return self._func(data.url, **kw)


class NumpyReader(NumpyText):
    func = "numpy:load"
    implements = {datatypes.NumpyFile}


class CupyNumpyReader(NumpyText):
    output_instance = "cupy:ndarray"
    implements = {datatypes.NumpyFile}
    imports = {"cupy"}
    func = "cupy:loadtxt"


class CupyTextReader(CupyNumpyReader):
    implements = {datatypes.Text}
    func = "numpy:loadtxt"


class XArrayDatasetReader(FileReader):
    output_instance = "xarray:Dataset"
    imports = {"xarray"}
    optional_imports = {"zarr", "h5netcdf", "cfgrib", "scipy"}  # and others
    # DAP is not a file but an API, maybe should be separate
    implements = {
        datatypes.NetCDF3,
        datatypes.HDF5,
        datatypes.GRIB2,
        datatypes.Zarr,
        datatypes.OpenDAP,
    }
    # xarray also reads from images and tabular data
    func = "xarray:open_mfdataset"
    other_funcs = {"xarray:open_dataset"}
    other_urls = {"xarray:open_dataset": "filename_or_obj"}
    url_arg = "paths"

    def _read(self, data, **kw):
        from xarray import open_dataset, open_mfdataset

        if "engine" not in kw and isinstance(data, datatypes.Zarr):
            kw["engine"] = "zarr"
            if data.root and "group" not in kw:
                kw["group"] = data.root
        if kw.get("engine", "") == "zarr":
            # only zarr takes storage options
            kw.setdefault("backend_kwargs", {})["storage_options"] = data.storage_options
        if isinstance(data, datatypes.HDF5) and data.path:
            kw["group"] = data.path
        if isinstance(data.url, (tuple, set, list)) or "*" in data.url:
            # use fsspec.open_files? (except for zarr)
            return open_mfdataset(data.url, **kw)
        else:
            # TODO: recognise fsspec URLs, and optionally use open_local for engines tha need it
            if kw.get("engine", "") == "h5netcdf" and data.url.startswith("http"):
                # special case, because xarray would assume a DAP endpoint
                f = fsspec.open(data.url, **(data.storage_options or {})).open()
                return open_dataset(f, **kw)
            return open_dataset(data.url, **kw)


class RasterIOXarrayReader(FileReader):
    output_instance = "xarray:Dataset"
    imports = {"rioxarray"}
    implements = {datatypes.TIFF, datatypes.GDALRasterFile}
    func = "rioxarray:open_rasterio"
    url_arg = "filename"

    def _read(self, data, concat_kwargs=None, **kwargs):
        import xarray as xr
        from rioxarray import open_rasterio

        concat_kwargs = concat_kwargs or {
            k: kwargs.pop(k)
            for k in {"dim", "data_vars", "coords", "compat", "position", "join"}
            if k in kwargs
        }

        with fsspec.open_files(data.url, **(data.storage_options or {})) as ofs:
            bits = [open_rasterio(of, **kwargs) for of in ofs]
        if len(bits) == 1:
            return bits
        else:
            # requires dim= in kwargs
            return xr.concat(bits, **concat_kwargs)


class GeoPandasReader(FileReader):
    # TODO: geopandas also supports postGIS
    output_instance = "geopandas:GeoDataFrame"
    imports = {"geopandas"}
    implements = {
        datatypes.GeoJSON,
        datatypes.CSV,
        datatypes.SQLite,
        datatypes.Shapefile,
        datatypes.GDALVectorFile,
    }
    func = "geopandas:read_file"
    url_arg = "filename"

    def _read(self, data, with_fsspec=None, **kwargs):
        import geopandas

        if with_fsspec is None:
            with_fsspec = (
                ("://" in data.url and "!" not in data.url)
                or "::" in data.url
                or data.storage_options
            )
        if with_fsspec:
            with fsspec.open(data.url, **(data.storage_options or {})) as f:
                return geopandas.read_file(f, **kwargs)
        return geopandas.read_file(data.url, **kwargs)


class GeoPandasTabular(FileReader):
    output_instance = "geopandas:GeoDataFrame"
    imports = {"geopandas", "pyarrow"}
    implements = {datatypes.Parquet, datatypes.Feather2}
    func = "geopands:read_parquet"
    other_funcs = {"geopands:read_feather"}

    def _read(self, data, **kwargs):
        import geopandas

        if "://" in data.url or "::" in data.url:
            f = fsspec.open(data.url, **(data.storage_options or {})).open()
        else:
            f = data.url
        if isinstance(data, datatypes.Parquet):
            return geopandas.read_parquet(f, **kwargs)
        elif isinstance(data, datatypes.Feather2):
            return geopandas.read_feather(f, **kwargs)
        else:
            raise ValueError


class ScipyMatlabReader(FileReader):
    output_instance = "numpy:ndarray"
    implements = {datatypes.MatlabArray}
    imports = {"scipy"}
    func = "scipy.io:loadmat"

    def _read(self, data, **kwargs):
        return self._func(data.path, appendmat=False, **kwargs)[data.variable]


class ScipyMatrixMarketReader(FileReader):
    output_instance = "scipy.sparse:coo_matrix"  # numpy-like
    implements = {datatypes.MatrixMarket}
    imports = {"scipy"}
    func = "scipy.io:mmread"

    def _read(self, data, **kw):
        with fsspec.open(data.url, **data.storage_options) as f:
            return self._func(f)


class NibabelNiftiReader(FileReader):
    # method img.get_fdata() gets the contents as numpy
    output_instance = "nibabel.spatialimages:SpatialImage"
    implements = {datatypes.Nifti}  # and other medical image types
    imports = {"nibabel"}
    func = "nibabel:load"
    url_arg = "filename"
    storage_options = True

    def _read(self, data, **kw):
        with fsspec.open(data.url, **(data.storage_options or {})) as f:
            return self._func(f, **kw)


class DicomReader(FileReader):
    output_instance = "pydicom.dataset:FileDataset"
    implements = {datatypes.DICOM}
    imports = {"pydicom"}
    func = "pydicom:read_file"
    url_arg = "fp"  # can be file-like
    storage_options = True

    def _read(self, data, **kw):
        with fsspec.open(data.url, **(data.storage_options or {})) as f:
            return self._func(f, **kw)


class Condition(BaseReader):
    def _read(
        self, if_true, if_false, condition: callable[[BaseReader, ...], bool] | bool, **kwargs
    ):
        if isinstance(condition, bool):
            cond = condition
        elif isinstance(condition, BaseReader):
            cond = condition.read()
        else:
            cond = condition(**kwargs)
        if cond:
            return if_true.read() if isinstance(if_true, BaseReader) else if_true
        else:
            return if_false.read() if isinstance(if_false, BaseReader) else if_false


class FileExistsReader(BaseReader):
    implements = {datatypes.FileData}
    func = "fsspec.core:url_to_fs"
    output_instance = "builtins:bool"

    def _read(self, data, *args, **kwargs):
        import fsspec

        try:
            fs, path = fsspec.core.url_to_fs(data.url, **(data.storage_options or {}))
        except FileNotFoundError:
            return False
        return fs.exists(path)


class YAMLCatalogReader(FileReader):
    implements = {datatypes.YAMLFile, datatypes.YAMLFile}
    func = "intake.readers.entry:Catalog.from_yaml_file"
    url_arg = "path"
    output_instance = "intake.readers.entry:Catalog"


class Retry(BaseReader):
    def _read(
        self,
        data,
        max_tries=10,
        allowed_exceptions=(Exception,),
        backoff0=0.1,
        backoff_factor=1.3,
        start_stage=None,
        end_stage=None,
        **kw,
    ):
        import time

        from intake.readers.convert import Pipeline

        if isinstance(allowed_exceptions, (list, set)):
            allowed_exceptions = tuple(allowed_exceptions)
        reader = data if isinstance(data, BaseReader) else data.reader
        if isinstance(reader, Pipeline) and (start_stage or end_stage):
            if start_stage:
                data = reader.first_n_stages(start_stage).read()
            else:
                data = reader.reader.read()
            for i in range(start_stage, end_stage):
                for i in range(max_tries):
                    try:
                        data = reader._read_stage_n(data, i)
                    except allowed_exceptions:
                        if i == max_tries - 1:
                            raise
                        time.sleep(backoff0 * backoff_factor**i)
                for j in range(end_stage, len(reader.steps) - 1):
                    data = reader._read_stage_n(data, j)
                return data
        else:
            for i in range(max_tries):
                try:
                    return reader.read()
                except allowed_exceptions:
                    if i == max_tries - 1:
                        raise
                    time.sleep(backoff0 * backoff_factor**i)


def recommend(data):
    """Show which readers claim to support the given data instance or a superclass

    The ordering is more specific readers first
    """
    seen = set()
    out = {"importable": [], "not_importable": []}
    for datacls in type(data).mro():
        for cls in subclasses(BaseReader):
            if any(datacls == imp for imp in cls.implements):
                if cls not in seen:
                    seen.add(cls)
                    if cls.check_imports():
                        out["importable"].append(cls)
                    else:
                        out["not_importable"].append(cls)
    return out


def reader_from_call(func: str, *args, join_lines=False, **kwargs) -> BaseReader:
    """Attempt to construct a reader instance by finding one that matches the function call

    Fails for readers that don't define a func, probably because it depends on the file
    type or needs a dynamic instance to be a method of.

    Parameters
    ----------
    func: callable | str
        If a callable, pass args and kwargs as you would have done to execute the function.
        If a string, it should look like "func(arg1, args2, kwarg1, **kw)", i.e., a normal
        python call but as a string. In the latter case, args and kwargs are ignored
    """

    import re
    from itertools import chain

    if isinstance(func, str):
        if join_lines:
            func = func.replace("\n", "")
        frame = inspect.currentframe().f_back
        match = re.match("^(.*=)?(.*?)[(](.*)[)]", func)
        if match:
            groups = match.groups()
        else:
            raise ValueError
        func = eval(groups[1], frame.f_globals, frame.f_locals)
        args, kwargs = eval(
            f"""(lambda *args, **kwargs: (args, kwargs))({groups[2]})""",
            frame.f_globals,
            frame.f_locals,
        )

    package = func.__module__.split(".", 1)[0]

    found = False
    for cls in subclasses(BaseReader):
        if cls.check_imports() and any(
            f.split(":", 1)[0].split(".", 1)[0] == package for f in ({cls.func} | cls.other_funcs)
        ):
            ffs = [f for f in ({cls.func} | cls.other_funcs) if import_name(f) == func]
            if ffs:
                found = cls
                func_name = ffs[0]
                break
    if not found:
        raise ValueError("Function not found in the set of readers")

    pars = inspect.signature(func).parameters
    kw = dict(zip(pars, args), **kwargs)
    data_kw = {}
    if issubclass(cls, FileReader):
        data_kw["storage_options"] = kw.pop("storage_options", None)
        data_kw["url"] = kw.pop(getattr(cls, "other_urls", {}).get(func_name, cls.url_arg))
    datacls = None
    if len(cls.implements) == 1:
        datacls = next(iter(cls.implements))
    elif getattr(cls, "url_arg", None):
        clss = datatypes.recommend(data_kw["url"], storage_options=data_kw["storage_options"])
        clss2 = [c for c in clss if c in cls.implements]
        if clss:
            datacls = next(iter(chain(clss2, clss)))
    if datacls:
        datacls = datacls(**data_kw)
        if data_kw["storage_options"] is None:
            del data_kw["storage_options"]
        cls = cls(datacls, **kwargs)
    else:
        url = data_kw.pop("url")
        cls = cls(url, **data_kw)

    return cls
