"""Classes for reading data into a python object"""

from __future__ import annotations

import inspect
import re

import fsspec

from intake import import_name
from intake.readers import datatypes
from intake.readers.mixins import PipelineMixin
from intake.readers.utils import Tokenizable, find_funcs, subclasses


class BaseReader(Tokenizable, PipelineMixin):
    imports: set[str] = set()  # allow for pip-style versions maybe
    implements: set[datatypes.BaseData] = set()
    optional_imports: set[str] = set()
    func: str = "builtins:NotImplementedError"  # usual function for loading data
    func_doc: str = None  # docstring origin if not from func
    concat_func: str = None  # for multi-file readers, this func can concatenate
    output_instance: str = None
    other_funcs: set = set()  # function names to recognise

    def __init__(self, data, metadata: dict | None = None, output_instance: str | None = None, **kwargs):
        """

        Parameters
        ----------
        data: intake.readers.datatypes.BaseData
        """
        self.data = data
        self.kwargs = kwargs
        self.metadata = data.metadata.copy()
        self.metadata.update(metadata or {})
        if output_instance:
            self.output_instance = output_instance

    def __repr__(self):
        return f"{type(self).__name__} reader for {self.data} producing {self.output_instance}"

    def __call__(self, *args, **kwargs):
        """New version of this instance with altered arguments"""
        kw = self.kwargs.copy()
        kw.update(kwargs)
        if args:
            kw["args"] = args
        return type(self)(data=self.data, **kw)

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

    def read(self, **kwargs):
        """Produce data artefact

        Any of the arguments encoded in the data instance can be overridden.

        Output type is given by the .output_instance attribute
        """
        kw = self.kwargs.copy()
        kw.update(kwargs)
        args = kw.pop("args", ())
        return self._read(*args, **kw)

    def _read(self, **kwargs):
        raise NotImplementedError

    def to_entry(self):
        """Create an entry with only this reader defined"""
        from intake.readers.entry import ReaderDescription

        return ReaderDescription(data=self.data.to_entry(), reader=self.qname(), kwargs=find_funcs(self.kwargs), output_instance=self.output_instance)


class FileReader(BaseReader):
    """Convenience superclass for readers of files"""

    url_arg = "url"
    other_urls = {}  # if we have other_funcs, may have different url_args for each
    storage_options = False

    def _read(self, **kw):
        kw[self.url_arg] = self.data.url
        if self.storage_options and self.data.storage_options:
            kw["storage_options"] = self.data.storage_options
        return self._func(**kw)


class FileByteReader(FileReader):
    """The contents of file(s) as bytes objects"""

    output_instance = "builtin:bytes"
    implements = {datatypes.FileData}

    def discover(self, **kwargs):
        with fsspec.open(self.data.url, mode="rb", **(self.data.storage_options or {})) as f:
            return f.read()

    def _read(self, **kwargs):
        out = []
        for of in fsspec.open_files(self.data.url, mode="rb", **(self.data.storage_options or {})):
            with of as f:
                out.append(f.read())
        return b"".join(out)


class Pandas(FileReader):
    imports = {"pandas"}
    concat_func = "pandas:concat"
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

    def _read(self, **kwargs):
        read_sql = import_name(self.func)
        return read_sql(sql=self.data.query, con=self.data.conn, **kwargs)


class DaskDF(FileReader):
    imports = {"dask", "pandas"}
    output_instance = "dask.dataframe:DataFrame"

    def to_dask(self, **kwargs):
        return self.read(**kwargs)


class DaskParquet(DaskDF):
    implements = {datatypes.Parquet}
    optional_imports = {"fastparquet", "pyarrow"}
    func = "dask.dataframe:read_parquet"
    url_arg = "path"


class DaskNPYStack(FileReader):
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

    def _read(self, **kwargs):
        return self._func(url=self.data.url, component=self.data.path or None, storage_options=self.data.storage_options, **kwargs)


class NumpyZarr(FileReader):
    implements = {datatypes.Zarr}
    imports = {"zarr"}
    output_instance = "numpy:ndarray"
    func = "zarr:open"

    def _read(self, **kwargs):
        return self._func(self.data.url, storage_options=kwargs.pop("storage_options"), path=self.data.path, **kwargs)[:]


class DuckDB(BaseReader):
    imports = {"duckdb"}
    output_instance = "duckdb:DuckDBPyRelation"  # can be converted to pandas with .df
    func_doc = "duckdb:query"
    _dd = {}  # hold the engines, so results are still valid

    def discover(self, **kwargs):
        return self.read().limit(10)

    def _duck(self):
        import duckdb

        conn = getattr(self.data, "conn", {})  # only SQL type normally has this
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
            self._dd[str(conn)] = duckdb.connect(**conn)  # connection must be cached for results to be usable
        return self._dd[str(conn)]


class DuckParquet(DuckDB, FileReader):
    implements = {datatypes.Parquet}

    def _read(self, **kwargs):
        return self._duck().query(f"SELECT * FROM read_parquet('{self.data.url}')")


class DuckCSV(DuckDB, FileReader):
    implements = {datatypes.CSV}

    def _read(self, **kwargs):
        return self._duck().query(f"SELECT * FROM read_csv_auto('{self.data.url}')")


class DuckJSON(DuckDB, FileReader):
    implements = {datatypes.JSONFile}

    def _read(self, **kwargs):
        return self._duck().query(f"SELECT * FROM read_json_auto('{self.data.url}')")


class DuckSQL(DuckDB):
    implements = {datatypes.SQLQuery}

    def _read(self, **kwargs):
        words = len(self.data.query.split())
        q = self.data.query if words > 1 else f"SELECT * FROM {self.data.query}"
        return self._duck().query(q)


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

    def _read(self, **kwargs):
        return self._spark().read.csv(self.data.url, **kwargs)


class SparkParquet(SparkDataFrame):
    implements = {datatypes.Parquet}

    def _read(self, **kwargs):
        return self._spark().read.parquet(self.data.url, **kwargs)


class SparkText(SparkDataFrame):
    implements = {datatypes.Text}

    def _read(self, **kwargs):
        return self._spark().read.text(self.data.url, **kwargs)


class Awkward(FileReader):
    imports = {"awkward"}
    output_instance = "awkward:Array"


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

    def discover(self, **kwargs):
        kw = {"nrows": 10, self.url_arg: self.data.url, "storage_options": self.data.storage_options}
        kw.update(kwargs)
        kw.pop("skipfooter", None)
        kw.pop("chuknsize", None)
        return self.read(**kw)


class PandasHDF5(Pandas):
    implements = {datatypes.HDF5}
    func = "pandas:read_hdf"
    imports = {"pandas", "pytables"}

    def _read(self, **kw):
        if self.data.storage_options:  # or fsspec-like
            with fsspec.open(self.data.url, "rb", **self.data.storage_options) as f:
                self._func(f, self.data.path, **kw)
        return self._func(self.data.url, **kw)


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


class TiledNode(BaseReader):
    implements = {datatypes.TiledService}
    imports = {"tiled"}
    output_instance = "tiled.client.node:Node"
    func = "tiled.client:from_uri"

    def _read(self, **kwargs):
        opts = self.data.options.copy()
        opts.update(kwargs)
        return self._func(self.data.url, **opts)


class TiledClient(BaseReader):
    # returns dask/normal x/array/dataframe
    implements = {datatypes.TiledDataset}
    output_instance = "tiled.client.base:BaseClient"

    def _read(self, as_client=True, dask=False, **kwargs):
        from tiled.client import from_uri

        opts = self.data.options.copy()
        opts.update(kwargs)
        if dask:
            opts["structure_clients"] = "dask"
        client = from_uri(self.data.url, **opts)
        if as_client:
            return client
        else:
            return client.read()


class PythonModule(BaseReader):
    output_instance = "builtins:module"
    implements = {datatypes.PythonSourceCode}

    def _read(self, module_name=None, **kwargs):
        from types import ModuleType

        if module_name is None:
            module_name = self.data.url.rsplit("/", 1)[-1].split(".", 1)[0]
        with fsspec.open(self.data.url, "rt", **(self.data.storage_options or {})) as f:
            mod = ModuleType(module_name)
            exec(f.read(), mod.__dict__)
            return mod


class SKImageReader(FileReader):
    output_instance = "numpy:ndarray"
    imports = {"scikit-image"}
    implements = {datatypes.PNG, datatypes.TIFF}
    func = "skimage.io:imread"
    url_arg = "fname"


class NumpyReader(FileReader):
    output_instance = "numpy:ndarray"
    implements = {datatypes.NumpyFile}
    imports = {"numpy"}
    func = "numpy:load"
    url_arg = "file"


class XArrayDatasetReader(FileReader):
    output_instance = "xarray:DataSet"
    imports = {"xarray"}
    optional_imports = {"zarr", "h5netcdf", "cfgrib", "scipy"}  # and others
    # DAP is not a file but an API, maybe should be separate
    implements = {datatypes.NetCDF3, datatypes.HDF5, datatypes.GRIB2, datatypes.Zarr, datatypes.OpenDAP}
    # xarray also reads from images and tabular data
    func = "xarray:open_mfdataset"
    other_funcs = {"xarray:open_dataset"}
    other_urls = {"xarray:open_dataset": "filename_or_obj"}
    url_arg = "paths"

    def _read(self, **kw):
        from xarray import open_dataset, open_mfdataset

        if "engine" not in kw and isinstance(self.data, datatypes.Zarr):
            kw["engine"] = "zarr"
        if kw.get("engine", "") == "zarr":
            # only zarr takes storage options
            kw.setdefault("backend_kwargs", {})["storage_options"] = self.data.storage_options
        if isinstance(self.data, datatypes.HDF5) and self.data.path:
            kw["group"] = self.data.path
        if isinstance(self.data.url, (tuple, set, list)) or "*" in self.data.url:
            # use fsspec.open_files? (except for zarr)
            return open_mfdataset(self.data.url, **kw)
        else:
            # TODO: recognise fsspec URLs, and optionally use open_local for engines tha need it
            if kw.get("engine", "") == "h5netcdf" and self.data.url.startswith("http"):
                # special case, because xarray would assume a DAP endpoint
                f = fsspec.open(self.data.url, **(self.data.storage_options or {})).open()
                return open_dataset(f, **kw)
            return open_dataset(self.data.url, **kw)


class RasterIOXarrayReader(FileReader):
    output_instance = "xarray:DataSet"
    imports = {"rioxarray"}
    implements = {datatypes.TIFF, datatypes.GDALRasterFile}
    func = "rioxarray:open_rasterio"
    url_arg = "filename"

    def _read(self, concat_kwargs=None, **kwargs):
        import xarray as xr
        from rioxarray import open_rasterio

        concat_kwargs = concat_kwargs or {k: kwargs.pop(k) for k in {"dim", "data_vars", "coords", "compat", "position", "join"} if k in kwargs}

        with fsspec.open_files(self.data.url, **(self.data.storage_options or {})) as ofs:
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
    implements = {datatypes.GeoJSON, datatypes.CSV, datatypes.SQLite, datatypes.Shapefile, datatypes.GDALVectorFile}
    func = "geopandas:read_file"
    url_arg = "filename"

    def _read(self, with_fsspec=None, **kwargs):
        import geopandas

        if with_fsspec is None:
            with_fsspec = ("://" in self.data.url and "!" not in self.data.url) or "::" in self.data.url or self.data.storage_options
        if with_fsspec:
            with fsspec.open(self.data.url, **(self.data.storage_options or {})) as f:
                return geopandas.read_file(f, **kwargs)
        return geopandas.read_file(self.data.url, **kwargs)


class GeoPandasTabular(FileReader):
    output_instance = "geopandas:GeoDataFrame"
    imports = {"geopandas", "pyarrow"}
    implements = {datatypes.Parquet, datatypes.Feather2}
    func = "geopands:read_parquet"
    other_funcs = {"geopands:read_feather"}

    def _read(self, **kwargs):
        import geopandas

        if "://" in self.data.url or "::" in self.data.url:
            f = fsspec.open(self.data.url, **(self.data.storage_options or {})).open()
        else:
            f = self.data.url
        if isinstance(self.data, datatypes.Parquet):
            return geopandas.read_parquet(f, **kwargs)
        elif isinstance(self.data, datatypes.Feather2):
            return geopandas.read_feather(f, **kwargs)
        else:
            raise ValueError


class Condition(BaseReader):
    implements = {datatypes.ReaderData}

    def _read(self, other: BaseReader, condition: callable[[BaseReader, ...], bool], **kwargs):
        if self.condition(self.data.reader, **self.kwargs):
            return self.data.reader.read()
        else:
            return self.other.read()


class Retry(BaseReader):
    implements = {datatypes.ReaderData}

    def _read(self, max_tries=10, allowed_exceptions=(Exception,), backoff0=0.1, backoff_factor=1.3, start_stage=None, end_stage=None, **kw):
        import time

        from intake.readers.convert import Pipeline

        reader = self.data if isinstance(self.data, BaseReader) else self.data.reader
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


def reader_from_call(func, *args, **kwargs):
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
        frame = inspect.currentframe().f_back
        match = re.match("^(.*?)[(](.*)[)]", func)
        if match:
            groups = match.groups()
        else:
            raise ValueError
        func = eval(groups[0], frame.f_globals, frame.f_locals)
        args, kwargs = eval(f"""(lambda *args, **kwargs: (args, kwargs))({groups[1]})""", frame.f_globals, frame.f_locals)

    package = func.__module__.split(".", 1)[0]

    found = False
    for cls in subclasses(BaseReader):
        if cls.check_imports() and any(f.split(":", 1)[0].split(".", 1)[0] == package for f in ({cls.func} | cls.other_funcs)):
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

    return {"reader": cls, "kwargs": kw, "data": datacls}
